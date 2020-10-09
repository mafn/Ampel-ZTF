#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/t0/ZTFAlertStreamController.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 07.08.2020
# Last Modified Date: 07.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import asyncio
import copy
from functools import partial
from typing import Any, Dict, Literal, Optional, Union, Iterable, Set, Sequence

from pydantic import ValidationError

from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.alert.load.TarAlertLoader import TarAlertLoader
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.core.AmpelContext import AmpelContext
from ampel.model.ProcessModel import ProcessModel
from ampel.model.Secret import Secret
from ampel.model.StrictModel import StrictModel
from ampel.model.UnitModel import UnitModel
from ampel.util import concurrent
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.ztf.t0.load.UWAlertLoader import UWAlertLoader
from ampel.ztf.archive.ArchiveDB import ArchiveDB
from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater


class ArchiveSink(StrictModel):
    db: str
    auth: Secret[dict] = {"key": "ztf/archive/writer"} # type: ignore[assignment]


class KafkaSource(StrictModel):
    broker: str
    group: str
    stream: Literal["ztf_uw_private", "ztf_uw_public"]
    timeout: int = 3600
    archive: Optional[ArchiveSink]

    def get(self) -> ZiAlertSupplier:
        supplier = ZiAlertSupplier(deserialize=None)
        supplier.set_alert_source(
            UWAlertLoader(
                partnership=(self.stream == "ztf_uw_private"),
                bootstrap=self.broker,
                group_name=self.group,
                archive_updater=(
                    ArchiveUpdater(self.archive.db, connect_args=self.archive.auth.get())
                    if self.archive else None
                ),
                timeout=self.timeout,
                ).alerts()
        )
        return supplier


class TarballSource(StrictModel):
    filename: str
    serialization: Optional[Literal["avro", "json"]] = "avro"

    def get(self) -> ZiAlertSupplier:
        supplier = ZiAlertSupplier(deserialize=self.serialization)
        supplier.set_alert_source(TarAlertLoader(self.filename))
        return supplier


class ArchiveSource(StrictModel):
    db: str
    group: Optional[str]
    stream: Literal["ztf_uw_private", "ztf_uw_public"]
    auth: Secret[dict] = {"key": "ztf/archive/reader"} # type: ignore[assignment]
    chunk: int = 5000
    jd_min: float = -float('inf')
    jd_max: float = float('inf')

    def get(self) -> ZiAlertSupplier:
        supplier = ZiAlertSupplier(deserialize=None)
        supplier.set_alert_source(
            ArchiveDB(self.db, connect_args=self.auth.get())
            .get_alerts_in_time_range(
                self.jd_min,
                self.jd_max,
                programid=(None if self.stream == "ztf_uw_private" else 1),
                group_name=f"{self.group}-{self.stream}" if self.group else None,
                block_size=self.chunk,
            )
        )
        return supplier


class AlertSource(StrictModel):
    source: Union[KafkaSource, TarballSource, ArchiveSource]

    def get(self) -> ZiAlertSupplier:
        return self.source.get()


class ZTFAlertStreamController(AbsProcessController):

    priority: str
    source: Union[KafkaSource, TarballSource, ArchiveSource]
    multiplier: int = 1

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        # merge process directives
        assert self.processes
        process = copy.deepcopy(self.processes[0])

        assert process.processor.unit == "AlertProcessor", "Lead process is an AlertProcessor"
        assert isinstance(process.processor.config, dict)

        def strip(config):
            """Remove AlertProcessor config keys that are either"""
            return {k: v for k, v in config.items() if k not in {"process_name", "publish_stats", "directives"}}

        for pm in self.processes[1:]:
            # ensure that trailing AlertProcessor configs are compatible
            assert process.controller == pm.controller
            assert isinstance(pm.processor.config, dict)
            assert process.processor.unit == pm.processor.unit, "All processes are AlertProcessors"
            assert strip(process.processor.config) == strip(pm.processor.config), "AlertProcessor configs are compatible"
            process.processor.config["directives"] += pm.processor.config["directives"]
        self.processes = [process]

        assert len(self.processes) == 1, "Can't handle multiple processes"
        self._scale_event = None

    async def run(self) -> None:
        self._scale_event = asyncio.Event()
        tasks = {
            asyncio.create_task(self.run_alertprocessor(pm, self._scale_event))
            for pm in self.processes
        }
        for results in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(results, BaseException):
                raise results
            for r in results:
                if isinstance(r, BaseException):
                    raise r

    def stop(self, name: Optional[str]=None) -> None:
        """Stop scheduling new processes."""
        assert name is None
        for pm in self.processes:
            pm.active = False

    def scale(self, name: Optional[str]=None, multiplier: int=1):
        if multiplier < 1:
            raise ValueError("multiplier must be nonnegative")
        assert self._scale_event
        self.processes[0].multiplier = multiplier
        self._scale_event.set()

    async def run_alertprocessor(self, pm: ProcessModel, scale_event: asyncio.Event) -> Sequence[bool]:
        """
        Keep `self.multiplier` instances of this process alive until cancelled
        """
        config = self.config.get()
        launch = partial(
            self.run_mp_process,
            config,
            self.secrets,
            pm.dict(),
            self.source.dict(exclude_defaults=False),
            self.log_profile,
        )
        assert pm.active
        pending = {launch() for _ in range(self.multiplier)}
        pending.add(asyncio.create_task(scale_event.wait(), name="scale"))
        done: Set[asyncio.Future] = set()
        try:
            while pm.active:
                try:
                    done, pending = await asyncio.wait(
                        pending, return_when="FIRST_COMPLETED"
                    )
                    for task in done:
                        if task.get_name() == "scale":
                            if scale_event.is_set():
                                print(f"scale {len(pending)} -> {pm.multiplier}")
                                # scale down
                                to_kill = {pending.pop() for _ in range(len(pending)-pm.multiplier)}
                                for t in to_kill:
                                    t.cancel()
                                await asyncio.gather(*to_kill, return_exceptions=True)
                                # scale up
                                for _ in range(pm.multiplier-len(pending)):
                                    pending.add(launch())
                                scale_event.clear()
                            pending.add(asyncio.create_task(scale_event.wait(), name="scale"))
                        else:
                            # start a fresh replica for each processor that
                            # returned True
                            if task.result() and len(pending) < pm.multiplier:
                                pending.add(launch())
                except:
                    for t in pending:
                        t.cancel()
                    break
        finally:
            # force scale future to come due
            scale_event.set()
            return await asyncio.gather(*done.union(pending), return_exceptions=True)

    @staticmethod
    @concurrent.process
    def run_mp_process(
        config: Dict[str, Any],
        secrets: Optional[AbsSecretProvider],
        p: Dict[str, Any],
        source: Dict[str, Any],
        log_profile: str = "default",
    ) -> bool:

        from ampel.alert.AlertProcessor import AlertProcessor

        # Create new context with frozen config
        context = AmpelContext.new(
            tier=p["tier"], config=AmpelConfig(config, freeze=True),
            secrets=secrets
        )

        processor = context.loader.new_admin_unit(
            unit_model=UnitModel(**p["processor"]),
            context=context,
            sub_type=AlertProcessor,
            log_profile=log_profile,
        )
        factory = AlertSource(
            **context.loader.resolve_secrets(
                context.loader.secrets,
                AlertSource,
                AlertSource.__annotations__,
                AlertSource.__field_defaults__,
                {'source': source},
            )
        )
        processor.set_supplier(factory.get())

        n_alerts = processor.run()

        if isinstance(factory.source, TarballSource):
            return False
        elif isinstance(factory.source, KafkaSource):
            return True
        else:
            return n_alerts > 0
