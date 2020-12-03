import itertools
from collections import defaultdict

import pytest

from ampel.alert.IngestionHandler import IngestionHandler
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.demo.unit.base.DemoLightCurveT2Unit import DemoLightCurveT2Unit
from ampel.ingest.PhotoCompoundIngester import PhotoCompoundIngester
from ampel.log.AmpelLogger import AmpelLogger, DEBUG
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.model.AlertProcessorDirective import AlertProcessorDirective
from ampel.model.UnitModel import UnitModel
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.ztf.ingest.ZiAlertContentIngester import ZiAlertContentIngester
from ampel.ztf.ingest.ZiT1ArchivalCompoundIngester import ZiT1ArchivalCompoundIngester
from ampel.ztf.ingest.ZiT1Combiner import ZiT1Combiner


def _make_ingester(context):
    run_id = 0
    logger = AmpelLogger.get_logger()
    updates_buffer = DBUpdatesBuffer(context.db, run_id=run_id, logger=logger)
    logd = LogsBufferDict({"logs": [], "extra": {}, "err": False,})

    ingester = context.loader.new_admin_unit(
        unit_model=UnitModel(unit=ZiT1ArchivalCompoundIngester),
        datapoint_ingester=UnitModel(unit=ZiAlertContentIngester),
        compound_ingester=UnitModel(
            unit=PhotoCompoundIngester, config={"combiner": {"unit": ZiT1Combiner}}
        ),
        updates_buffer=updates_buffer,
        logd=logd,
        run_id=run_id,
        context=context,
    )

    return ingester


def get_supplier(loader):
    supplier = ZiAlertSupplier(deserialize="avro")
    supplier.set_alert_source(loader)
    return supplier


def raw_alert_dicts(loader):
    supplier = get_supplier(loader)
    for payload in supplier.alert_loader:
        yield supplier.deserialize(payload)


def consolidated_alert(alerts):
    """
    Make one mega-alert containing all photopoints for an object, similar to
    the one returned by ArchiveDB.get_photopoints_for_object
    """
    candidates = []
    prv_candidates = []
    upper_limits = []
    for alert in alerts:
        oid = alert["objectId"]
        candidates.append((oid, alert["candidate"]))
        for prv in alert["prv_candidates"]:
            if prv.get("magpsf") is None:
                upper_limits.append((oid, prv))
            else:
                prv_candidates.append((oid, prv))
    # ensure exactly one observation per jd. in case of conflicts, sort by
    # candidate > prv_candidate > upper_limit, then pid
    photopoints = defaultdict(dict)
    for row in ([upper_limits], [prv_candidates], [candidates]):
        for pp in sorted(row[0], key=lambda pp: (pp[0], pp[1]["jd"], pp[1]["pid"])):
            photopoints[pp[0]][pp[1]["jd"]] = pp[1]
    assert len(photopoints) == 1
    objectId = list(photopoints.keys())[0]
    datapoints = sorted(
        photopoints[objectId].values(), key=lambda pp: pp["jd"], reverse=True
    )
    candidate = datapoints.pop(0)
    return {
        "objectId": objectId,
        "candid": candidate["candid"],
        "programid": candidate["programid"],
        "candidate": candidate,
        "prv_candidates": datapoints,
    }


def test_instantiate(patch_mongo, dev_context, mocker):
    mock = mocker.patch("ampel.ztf.ingest.ZiT1ArchivalCompoundIngester.ArchiveDB")
    _make_ingester(dev_context)
    assert mock.called_once()


@pytest.fixture
def mock_ingester(patch_mongo, dev_context, mocker, avro_packets):
    mock = mocker.patch("ampel.ztf.ingest.ZiT1ArchivalCompoundIngester.ArchiveDB")
    ingester = _make_ingester(dev_context)
    assert mock.called_once()
    # mock archivedb to return first alert
    ingester.archive.configure_mock(
        **{
            "get_photopoints_for_object.return_value": consolidated_alert(
                raw_alert_dicts(itertools.islice(avro_packets(), 0, 1))
            )
        }
    )
    return ingester


def test_ingest_previous_alerts(mock_ingester, avro_packets):
    """ingest_previous_alerts() populates the t0 collection"""

    alerts = list(get_supplier(avro_packets()))
    datapoints = mock_ingester.datapoint_engine.ingest(alerts[1])

    assert mock_ingester.context.db.get_collection("t0").count_documents({}) == len(
        alerts[1].dps
    )

    mock_ingester.ingest_previous_alerts(alerts[1].stock_id, datapoints)
    assert mock_ingester.archive.get_alerts_for_object.called_once()

    assert mock_ingester.context.db.get_collection("t0").count_documents({}) == sum(
        len(alert.dps) for alert in alerts[:2]
    )


def test_get_earliest_jd(mock_ingester, avro_packets):
    """earliest jd is stable under out-of-order ingestion"""

    alerts = list(get_supplier(avro_packets()))

    datapoints = mock_ingester.datapoint_engine.ingest(alerts[2])
    assert mock_ingester.get_earliest_jd(alerts[2].stock_id, datapoints) == min(
        dp["jd"] for dp in alerts[2].pps
    ), "min jd is min jd of last ingested alert"

    datapoints = mock_ingester.datapoint_engine.ingest(alerts[0])
    assert mock_ingester.get_earliest_jd(alerts[0].stock_id, datapoints) == min(
        [dp["jd"] for dp in alerts[0].pps]
    ), "min jd is min jd of last ingested alert"

    datapoints = mock_ingester.datapoint_engine.ingest(alerts[1])
    assert mock_ingester.get_earliest_jd(alerts[1].stock_id, datapoints) == min(
        [dp["jd"] for dp in alerts[0].pps]
    ), "min jd is min jd of earliest ingested alert"


def test_add_channel(mock_ingester):
    mock_ingester.add_channel("EXAMPLE_TNS_MSIP")
    assert "EXAMPLE_TNS_MSIP" in mock_ingester.channels


def test_ingest(mock_ingester, avro_packets):
    """ingest() populates the t0 collection with archival points"""

    alerts = list(get_supplier(avro_packets()))
    datapoints = mock_ingester.datapoint_engine.ingest(alerts[1])

    blueprint = mock_ingester.ingest(
        alerts[1].stock_id, datapoints, [("EXAMPLE_TNS_MSIP", True)]
    )
    assert (
        not mock_ingester.archive.get_alerts_for_object.called
    ), "short-cut if no channels match"
    assert blueprint == type(blueprint)(), "default (empty) blueprint returned"

    mock_ingester.add_channel("EXAMPLE_TNS_MSIP")
    blueprint = mock_ingester.ingest(
        alerts[1].stock_id, datapoints, [("EXAMPLE_TNS_MSIP", True)]
    )
    assert blueprint != type(blueprint)()
    assert mock_ingester.archive.get_alerts_for_object.called_once()

    assert mock_ingester.context.db.get_collection("t0").count_documents({}) == sum(
        len(alert.dps) for alert in alerts[:2]
    ), "archival points ingested"


def get_handler(context, directives):
    run_id = 0
    logger = AmpelLogger.get_logger(console={"level": DEBUG})
    updates_buffer = DBUpdatesBuffer(context.db, run_id=run_id, logger=logger)
    logd = LogsBufferDict({"logs": [], "extra": {}})
    return IngestionHandler(
        context=context,
        logger=logger,
        run_id=0,
        updates_buffer=updates_buffer,
        directives=directives,
    )


def test_integration(patch_mongo, dev_context, mocker, avro_packets):
    directive = {
        "channel": "EXAMPLE_TNS_MSIP",
        "stock_update": {"unit": "ZiStockIngester"},
        "t0_add": {
            "ingester": "ZiAlertContentIngester",
            "t1_combine": [
                {
                    "ingester": "PhotoCompoundIngester",
                    "config": {"combiner": {"unit": "ZiT1Combiner"}},
                    "t2_compute": {
                        "ingester": "PhotoT2Ingester",
                        "units": [{"unit": DemoLightCurveT2Unit}],
                    },
                }
            ],
        },
        "t1_combine": [
            {
                "ingester": ZiT1ArchivalCompoundIngester,
                "config": {
                    "datapoint_ingester": {"unit": "ZiAlertContentIngester"},
                    "compound_ingester": {
                        "unit": "PhotoCompoundIngester",
                        "config": {"combiner": {"unit": "ZiT1Combiner"}},
                    },
                },
                "t2_compute": {
                    "ingester": "PhotoT2Ingester",
                    "units": [{"unit": DemoLightCurveT2Unit}],
                },
            }
        ],
    }
    mock = mocker.patch("ampel.ztf.ingest.ZiT1ArchivalCompoundIngester.ArchiveDB")
    handler = get_handler(dev_context, [AlertProcessorDirective(**directive)])
    ingester = next(iter(handler.t1_ingesters.keys()))
    assert isinstance(ingester, ZiT1ArchivalCompoundIngester)

    # mock archivedb to return first alert
    ingester.archive.configure_mock(
        **{
            "get_photopoints_for_object.return_value": consolidated_alert(
                raw_alert_dicts(itertools.islice(avro_packets(), 0, 1))
            )
        }
    )

    t0 = dev_context.db.get_collection("t0")
    t1 = dev_context.db.get_collection("t1")
    t2 = dev_context.db.get_collection("t2")
    assert t0.count_documents({}) == 0

    alerts = list(get_supplier(avro_packets()))
    handler.ingest(alerts[1], [("EXAMPLE_TNS_MSIP", True)])

    # note lack of handler.updates_buffer.push_updates() here;
    # ZiAlertContentIngester has to be synchronous to deal with superseded
    # photopoints
    assert t0.count_documents({}) == len(alerts[1].dps) + len(
        alerts[0].dps
    ), "datapoints ingested for archival alert"

    # now push; the rest are asynchronous
    handler.updates_buffer.push_updates()

    assert t1.count_documents({}) == 2, "two compounds produced"
    assert t2.count_documents({}) == 2, "two t2 docs produced"

    assert t2.find_one({"link": t1.find_one({"len": len(alerts[1].dps)})["_id"]})
    assert t2.find_one(
        {"link": t1.find_one({"len": len(alerts[1].dps) + len(alerts[0].dps)})["_id"]}
    )
