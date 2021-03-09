import pytest
from pathlib import Path
import yaml
import logging
import requests

from ampel.ztf.t2.T2CatalogMatch import T2CatalogMatch
from ampel.content.DataPoint import DataPoint


@pytest.fixture
def catalogmatch_config():
    with open(Path(__file__).parent / "test-data" / "catalogmatch_config.yaml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def catalogmatch_service_reachable():
    try:
        requests.head("https://ampel.zeuthen.desy.de/", timeout=0.5)
    except requests.exceptions.Timeout:
        pytest.skip("https://ampel.zeuthen.desy.de/ is unreachable")


def test_match(
    patch_mongo, dev_context, catalogmatch_config, catalogmatch_service_reachable
):
    unit = T2CatalogMatch(
        dev_context, logger=logging.getLogger(), **catalogmatch_config
    )
    result = unit.run(DataPoint({"_id": 0, "body": {"ra": 0, "dec": 0}}))
    assert result == {
        **{k: None for k in catalogmatch_config["catalogs"].keys()},
        **{
            "SDSS_spec": {
                "bptclass": 5.0,
                "dist2transient": 0.0,
                "subclass": "n/a",
                "z": 0.06367020308971405,
            },
            "GLADEv23": {
                "dist2transient": 0.0,
                "flag1": "G",
                "dist": None,
                "dist_err": None,
                "z": None,
                "flag2": 0,
                "flag3": 0,
            },
        },
    }
