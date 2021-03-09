import pytest
import yaml
from pathlib import Path

from ampel.model.ZTFLegacyChannelTemplate import ZTFLegacyChannelTemplate
from ampel.model.AlertProcessorDirective import AlertProcessorDirective
from ampel.log.AmpelLogger import AmpelLogger

@pytest.fixture
def logger():
    return AmpelLogger.get_logger()

def test_alert_only(logger, first_pass_config):
    template = ZTFLegacyChannelTemplate(
        **{
            "channel": "EXAMPLE_TNS_MSIP",
            "contact": "ampel@desy.de",
            "active": True,
            "auto_complete": False,
            "template": "ztf_uw_public",
            "t0_filter": {"unit": "NoFilter"},
        }
    )
    process = template.get_processes(logger=logger, first_pass_config=first_pass_config)[0]
    assert process["tier"] == 0
    directive = AlertProcessorDirective(**process["processor"]["config"]["directives"][0])
    assert directive.t0_add
    assert directive.stock_update
    assert len(directive.t0_add.t1_combine) == 1
    assert len(units := directive.t0_add.t1_combine[0].t2_compute.units) == 1
    assert units[0].unit == "T2LightCurveSummary"
    assert not directive.t1_combine

def test_alert_t2(logger, first_pass_config):
    template = ZTFLegacyChannelTemplate(
        **{
            "channel": "EXAMPLE_TNS_MSIP",
            "contact": "ampel@desy.de",
            "active": True,
            "auto_complete": False,
            "template": "ztf_uw_public",
            "t0_filter": {"unit": "NoFilter"},
            "t2_compute": {"unit": "DemoLightCurveT2Unit",}
        }
    )
    process = template.get_processes(logger=logger, first_pass_config=first_pass_config)[0]
    assert process["tier"] == 0
    directive = AlertProcessorDirective(**process["processor"]["config"]["directives"][0])
    assert directive.t0_add
    assert directive.stock_update
    assert len(directive.t0_add.t1_combine) == 1
    assert len(units := directive.t0_add.t1_combine[0].t2_compute.units) == 2
    assert {u.unit for u in units} == {"DemoLightCurveT2Unit", "T2LightCurveSummary"}
    assert not directive.t1_combine

def test_archive_t2(logger, first_pass_config):
    template = ZTFLegacyChannelTemplate(
        **{
            "channel": "EXAMPLE_TNS_MSIP",
            "contact": "ampel@desy.de",
            "active": True,
            "auto_complete": False,
            "template": "ztf_uw_public",
            "t0_filter": {"unit": "NoFilter"},
            "t2_compute": {"archive": [{"unit": "DemoLightCurveT2Unit",}]}
        }
    )
    process = template.get_processes(logger=logger, first_pass_config=first_pass_config)[0]
    assert process["tier"] == 0
    directive = AlertProcessorDirective(**process["processor"]["config"]["directives"][0])
    assert directive.t0_add
    assert directive.stock_update
    assert len(units := directive.t0_add.t1_combine[0].t2_compute.units) == 1
    assert units[0].unit == "T2LightCurveSummary"
    assert len(directive.t1_combine) == 1
    assert len(units := directive.t1_combine[0].t2_compute.units) == 1
    assert units[0].unit == "DemoLightCurveT2Unit"
