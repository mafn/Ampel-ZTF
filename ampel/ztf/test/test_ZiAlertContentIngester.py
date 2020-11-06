import itertools
from functools import partial
from pathlib import Path

import mongomock
import pytest

from ampel.alert.load.TarAlertLoader import TarAlertLoader
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.ztf.ingest.ZiAlertContentIngester import ZiAlertContentIngester


@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.db.AmpelDB.MongoClient", mongomock.MongoClient)


@pytest.fixture
def dev_context(patch_mongo):
    return DevAmpelContext.load(
        Path(__file__).parent / "test-data" / "testing-config.yaml"
    )


@pytest.fixture
def avro_packets():
    """
    4 alerts for a random AGN, widely spaced:
    
    ------------------ -------------------------- ------------------------
    candid             detection                  history
    ------------------ -------------------------- ------------------------
    673285273115015035 2018-11-05 06:50:48.001935 29 days, 22:11:31.004165 
    879461413115015009 2019-05-30 11:04:25.996800 0:00:00 
    882463993115015007 2019-06-02 11:08:09.003839 3 days, 0:03:43.007039 
    885458643115015010 2019-06-05 11:00:26.997131 5 days, 23:56:01.000331 
    ------------------ -------------------------- ------------------------
    """
    return partial(
        TarAlertLoader, Path(__file__).parent / "test-data" / "ZTF18abxhyqv.tar.gz"
    )

@pytest.fixture
def superseded_packets():
    """
    Two alerts, received 100 ms apart, with the same points but different candids
    """
    return partial(
        TarAlertLoader, Path(__file__).parent / "test-data" / "ZTF18acruwxq.tar.gz"
    )


@pytest.fixture
def ingester(dev_context):
    """
    Set up ZiAlertContentIngester
    """
    run_id = 0
    logger = AmpelLogger.get_logger()
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=logger)
    logd = LogsBufferDict({"logs": [], "extra": {}, "err": False,})

    ingester = ZiAlertContentIngester(
        updates_buffer=updates_buffer, logd=logd, run_id=run_id, context=dev_context,
    )

    return ingester


def get_supplier(loader):
    supplier = ZiAlertSupplier(deserialize="avro")
    supplier.set_alert_source(loader)
    return supplier


def test_deduplication(ingester, avro_packets):
    """
    Database gets only one copy of each datapoint
    """

    alerts = list(get_supplier(itertools.islice(avro_packets(), 1, None)))

    pps = []
    uls = []
    for alert in alerts:
        pps += alert.get_tuples("jd", "fid", data="pps")
        uls += alert.get_values("jd", data="uls")
        ingester.ingest(alert)

    assert len(set(uls)) < len(uls), "Some upper limits duplicated in alerts"
    assert len(set(pps)) < len(pps), "Some photopoints duplicated in alerts"

    ingester.updates_buffer.push_updates()

    t0 = ingester.context.db.get_collection("t0")
    assert t0.count_documents({"_id": {"$gt": 0}}) == len(set(pps))
    assert t0.count_documents({"_id": {"$lt": 0}}) == len(set(uls))


def test_out_of_order_ingestion(ingester, avro_packets):
    """
    Ensure that returned alert content does not depend on whether photopoints
    were already committed to the database
    """

    alerts = list(get_supplier(avro_packets()))

    assert alerts[-1].pps[0]["jd"] > alerts[-2].pps[0]["jd"]

    def ingest(alert):
        dps = ingester.ingest(alert)
        ingester.updates_buffer.push_updates()
        return dps

    in_order = {idx: ingest(alerts[idx]) for idx in (-3,-1,-2)}

    # clean up mutations
    ingester.context.db.get_collection("t0").delete_many({})
    alerts = list(get_supplier(avro_packets()))

    out_of_order = {idx: ingest(alerts[idx]) for idx in (-3,-2,-1)}

    assert sorted(in_order.items()) == sorted(out_of_order.items())

@pytest.mark.xfail(reason="Current ZiAlertContentIngester reprocessing check has a data race")
def test_superseded_candidates(ingester, superseded_packets):
    """
    Ensure that photopoints are marked superseded even if ingested simultaneously
    """

    alerts = list(reversed(list(get_supplier(superseded_packets()))))

    assert alerts[0].pps[0]["jd"] == alerts[1].pps[0]["jd"]
    candids = [alert.pps[0]["candid"] for alert in alerts]
    assert candids[0] < candids[1]

    def ingest(alert):
        dps = ingester.ingest(alert)
        print(ingester.updates_buffer.db_ops['t0'])
        ingester.updates_buffer.push_updates()
        return dps

    # NB: commit updates only after ingestion
    dps = [ingester.ingest(alert) for alert in alerts]
    ingester.updates_buffer.push_updates()

    pp_db = ingester.context.db.get_collection("t0").find_one({"_id" : candids[0]}, ingester.projection)

    assert dps[0][0]["tag"] == pp_db["tag"], "data points match database content"
    assert "SUPERSEDED" in pp0["tag"], "database point marked as superseded"


