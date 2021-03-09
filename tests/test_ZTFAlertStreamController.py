import asyncio
import copy
import os
import time

import pytest

from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.log.AmpelLogger import AmpelLogger
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.ProcessModel import ProcessModel
from ampel.model.ZTFLegacyChannelTemplate import ZTFLegacyChannelTemplate
from ampel.util import concurrent
from ampel.ztf.t0.ZTFAlertStreamController import ZTFAlertStreamController


def t0_process(kwargs, first_pass_config):
    first_pass_config["resource"]["ampel-ztf/kafka"] = {"group": "nonesuch", "broker": "nonesuch:9092"}
    return ProcessModel(
        **ZTFLegacyChannelTemplate(**kwargs).get_processes(
            AmpelLogger.get_logger(), first_pass_config
        )[0]
    )


def make_controller(config, processes, klass=ZTFAlertStreamController):
    return klass(
        config=config,
        priority="standard",
        source={"stream": "ztf_uw_public", **config.get("resource.ampel-ztf/kafka")},
        processes=processes,
    )


@pytest.fixture
def config(first_pass_config):
    first_pass_config["resource"]["ampel-ztf/kafka"] = {
        "broker": "nonesuch:9092",
        "group": "nonesuch",
    }
    return AmpelConfig(dict(first_pass_config))


def test_merge_processes(config, first_pass_config):
    # matching processes
    processes = [
        t0_process(
            {
                "channel": name,
                "auto_complete": False,
                "template": "ztf_uw_public",
                "t0_filter": {"unit": "NoFilter"},
            },
            first_pass_config,
        )
        for name in ("foo", "bar")
    ]
    len(
        ZTFAlertStreamController.merge_processes(processes).processor.config[
            "directives"
        ]
    ) == 2

    # incompatible processor configs
    processes[0].processor.config["iter_max"] = 100
    processes[1].processor.config["iter_max"] = 200
    with pytest.raises(AssertionError):
        make_controller(config, processes)

    # incompatible controller configs
    processes = [
        t0_process(
            {
                "channel": name,
                "auto_complete": False,
                "template": stream,
                "t0_filter": {"unit": "NoFilter"},
            },
            first_pass_config,
        )
        for name, stream in zip(("foo", "bar"), ("ztf_uw_private", "ztf_uw_public"))
    ]
    with pytest.raises(AssertionError):
        make_controller(config, processes)


class PotemkinZTFAlertStreamController(ZTFAlertStreamController):
    @staticmethod
    @concurrent.process
    def run_mp_process(
        config, secrets, p, source, log_profile: str = "default",
    ) -> bool:
        print(f"{os.getpid()} is sleepy...")
        time.sleep(1)
        print(f"{os.getpid()} is done!")
        return True


@pytest.fixture
def potemkin_controller(config, first_pass_config):
    processes = [
        t0_process(
            {
                "channel": name,
                "active": True,
                "auto_complete": False,
                "template": "ztf_uw_public",
                "t0_filter": {"unit": "NoFilter"},
            },
            first_pass_config,
        )
        for name in ("foo", "bar")
    ]
    controller = make_controller(config, processes, PotemkinZTFAlertStreamController)
    assert controller.processes[0].active
    return controller


@pytest.mark.asyncio
async def test_process_gauge(potemkin_controller):

    process_count = lambda: AmpelMetricsRegistry.registry().get_sample_value(
        "ampel_processes", {"tier": "0", "process": potemkin_controller._process.name}
    )

    r = asyncio.create_task(potemkin_controller.run())
    try:
        await asyncio.wait_for(asyncio.shield(r), 0.5)
    except asyncio.exceptions.TimeoutError:
        ...
    try:
        assert process_count() == 1
    finally:
        potemkin_controller.stop()
        await r
    assert process_count() == 0


@pytest.mark.asyncio
async def test_scale(potemkin_controller):

    process_count = lambda: AmpelMetricsRegistry.registry().get_sample_value(
        "ampel_processes", {"tier": "0", "process": potemkin_controller._process.name}
    )
    try:
        r = asyncio.create_task(potemkin_controller.run())
        await asyncio.sleep(0.5)
        assert process_count() == 1
        potemkin_controller.scale(multiplier=2)
        await asyncio.sleep(2)
        assert process_count() == 2
        potemkin_controller.scale(multiplier=3)
        await asyncio.sleep(2)
        assert process_count() == 3
        potemkin_controller.scale(multiplier=1)
        await asyncio.sleep(1)
        assert process_count() == 1
    finally:
        potemkin_controller.stop()
    await r


@pytest.mark.asyncio
async def test_stop(potemkin_controller):

    r = asyncio.create_task(potemkin_controller.run())
    try:
        await asyncio.wait_for(asyncio.shield(r), 0.5)
    except asyncio.exceptions.TimeoutError:
        ...
    potemkin_controller.stop()
    assert isinstance((await r)[0], asyncio.CancelledError)
