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
from typing import Any, Dict, Literal, Optional, Union

from ampel.abstract.AbsProcessController import AbsProcessController
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.alert.load.TarAlertLoader import TarAlertLoader
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.core.AmpelContext import AmpelContext
from ampel.model.ProcessModel import ProcessModel
from ampel.model.StrictModel import StrictModel
from ampel.model.UnitModel import UnitModel
from ampel.util import concurrent
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier


class KafkaSource(StrictModel):
    broker: str
    group: str
    stream: str


class TarballSource(StrictModel):
    filename: str
    serialization: Optional[Literal["avro", "json"]] = "avro"


class ZTFAlertStreamController(AbsProcessController, AmpelBaseModel):

    priority: str
    source: Union[KafkaSource, TarballSource]
    multiplier: int = 1

    def __init__(self, config, processes, *args, **kwargs):
        AbsProcessController.__init__(self, config, processes)
        AmpelBaseModel.__init__(self, **kwargs)

        # merge process directives
        assert self.proc_models
        process = copy.deepcopy(self.proc_models[0])
        # FIXME: check that trailing processes have compatible AlertProcessor
        # configs
        for pm in self.proc_models[1:]:
            process.directives += pm.directives
        self.proc_models = [process]

        assert len(self.proc_models) == 1, "Can't handle multiple processes"

    async def run(self) -> None:
        tasks = {
            asyncio.create_task(self.run_alertprocessor(pm))
            for pm in self.proc_models
        }
        for r in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(r, Exception):
                raise r

    async def run_alertprocessor(self, pm: ProcessModel):
        """
        Keep `self.multiplier` instances of this process alive until cancelled
        """
        config = self.config.get()
        launch = partial(
            self.run_mp_process,
            config,
            pm.dict(),
            self.source.dict(exclude_defaults=False),
            self.log_profile,
        )
        pending = {launch() for _ in range(self.multiplier)}
        done = {}
        while True:
            try:
                done, pending = await asyncio.wait(
                    pending, return_when="FIRST_COMPLETED"
                )
            except asyncio.CancelledError:
                try:
                    for t in pending:
                        t.cancel()
                finally:
                    break
            for result in done:
                if isinstance(result, BaseException):
                    # TODO: something useful with exceptions
                    ...
            # start a fresh replica for each one that finished
            for _ in range(self.multiplier - len(pending)):
                pending.add(launch())
        return await asyncio.gather(*pending, return_exceptions=True)

    @staticmethod
    @concurrent.process
    def run_mp_process(
        config: Dict[str, Any],
        p: Dict[str, Any],
        source: Dict[str, Any],
        log_profile: str = "default",
    ) -> Any:

        # Create new context with frozen config
        context = AmpelContext.new(
            tier=p["tier"], config=AmpelConfig(config, freeze=True)
        )

        processor = context.loader.new_admin_unit(
            unit_model=UnitModel(**p["processor"]),
            context=context,
            sub_type=AbsProcessorUnit,
            log_profile=log_profile,
        )
        if "filename" in source:
            processor.set_supplier(ZiAlertSupplier(deserialize=source["serialization"]))
            processor.set_loader(TarAlertLoader(source["filename"]))
        else:
            raise ValueError

        return processor.run()
