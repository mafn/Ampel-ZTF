from ampel.core.AmpelContext import AmpelContext
from ampel.model.UnitModel import UnitModel
import pytest
from pathlib import Path
import yaml
import logging
import requests

from ampel.ztf.t3.complement.TNSNames import TNSNames
from ampel.ztf.t3.complement.TNSReports import TNSReports
from ampel.ztf.t2.T2CatalogMatch import T2CatalogMatch
from ampel.ztf.t0.DecentFilter import DecentFilter

from ampel.content.DataPoint import DataPoint
from ampel.content.StockDocument import StockDocument
from ampel.content.T2Document import T2Document

from ampel.struct.AmpelBuffer import AmpelBuffer


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


def test_catalogmatch(
    patch_mongo,
    dev_context: AmpelContext,
    catalogmatch_config,
    catalogmatch_service_reachable,
):
    unit, _ = dev_context.loader.new_logical_unit(
        UnitModel(unit=T2CatalogMatch, config=catalogmatch_config),
        logger=logging.getLogger(),
    )
    result = unit.run(DataPoint({"id": 0, "body": {"ra": 0, "dec": 0}}))
    assert result ==
        {k: None for k in catalogmatch_config["catalogs"].keys()} |
        {
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
        }
    }


def test_decentfilter_star_in_gaia(patch_mongo, dev_context: AmpelContext):
    with open(Path(__file__).parent / "test-data" / "decentfilter_config.yaml") as f:
        config = yaml.safe_load(f)
    unit, _ = dev_context.loader.new_logical_unit(
        UnitModel(unit=DecentFilter, config=config), logger=logging.getLogger()
    )
    assert unit.is_star_in_gaia(
        {"ra": 0.009437700971970959, "dec": -0.0008937364197194631}
    )
    assert not unit.is_star_in_gaia({"ra": 0, "dec": 0})


def test_tnsnames(patch_mongo, dev_context) -> None:
    unit = dev_context.loader.new_context_unit(
        UnitModel(unit=TNSNames),
        logger=logging.getLogger(),
        context=dev_context,
    )
    buf = AmpelBuffer(
        {
            "id": 0,
            "stock": StockDocument(
                {
                    "_id": 0,
                    "tag": None,
                    "channel": None,
                    "journal": [],
                    "updated": {},
                    "created": {},
                    "name": ["sourceysource"],
                }
            ),
            "t2": [
                T2Document(
                    {
                        "_id": 0,
                        "unit": "T2LightCurveSummary",
                        "body": [
                            {"ts": 0, 'data': {"ra": 0.518164, "dec": 0.361964}}
                        ],
                    }
                )
            ],
        }
    )
    unit.complement([buf])
    assert buf["stock"]["name"] == ("sourceysource", "TNS2020ubb")
    assert not "extra" in buf

    unit = dev_context.loader.new_context_unit(
        UnitModel(unit=TNSReports),
        logger=logging.getLogger(),
        context=dev_context,
    )
    unit.complement([buf])
    assert buf["stock"]["name"] == ("sourceysource", "TNS2020ubb")
    assert buf["extra"] == {
        "TNSReports": [
            {
                "decdeg_err": 2.5e-05,
                "discmagfilter": {"id": 111, "name": "r", "family": "ZTF"},
                "discoverer": "F. Forster, A. Munoz-Arancibia, F.E. Bauer, L. Hernandez-Garcia, L. Galbany, G. Pignata, E. Camacho, J. Silva-Farfan, A. Mourao, J. Arredondo, G. Cabrera-Vives, R. Carrasco-Davis, P.A. Estevez, P. Huijse, E. Reyes, I. Reyes, P. Sanchez-Saez, C. Valenzuela, E. Castillo, D. Ruz-Mieres, D. Rodriguez-Mancini, M. Catelan, S. Eyheramendy, M.J. Graham on behalf of the ALeRCE broker",
                "discoverer_internal_name": "ZTF20acddzrd",
                "discovery_data_source": {"groupid": 48, "group_name": "ZTF"},
                "discoverydate": "2020-09-23T08:28:24.004000",
                "discoverymag": 19.1521,
                "end_prop_period": None,
                "host_redshift": 0.301422,
                "hostname": "WISEA J000204.63+002140.7",
                "internal_names": "ZTF20acddzrd",
                "name_prefix": "AT",
                "object_type": {"name": None, "id": None},
                "objid": 67022,
                "objname": "2020ubb",
                "public": 1,
                "radeg_err": 2.5e-05,
                "redshift": None,
                "reporter": "ALeRCE",
                "reporterid": 66140,
                "reporting_group": {"groupid": 74, "group_name": "ALeRCE"},
                "source": "bot",
            }
        ]
    }
