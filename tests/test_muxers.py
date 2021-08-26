import itertools
import os
from collections import defaultdict
from ampel.alert.PhotoAlert import PhotoAlert

import fastavro
from pymongo.operations import UpdateOne
import pytest
from ampel.util import concurrent
from ampel.core.AmpelContext import AmpelContext
from ampel.dev.UnitTestAlertSupplier import UnitTestAlertSupplier
from ampel.ingest.ChainedIngestionHandler import ChainedIngestionHandler
from ampel.ingest.T0Compiler import T0Compiler
from ampel.log.AmpelLogger import DEBUG, AmpelLogger
from ampel.model.ingest.IngestDirective import IngestDirective
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.mongo.update.MongoT0Ingester import MongoT0Ingester
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.DictSecretProvider import DictSecretProvider
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.ztf.ingest.ZiArchiveMuxer import ZiArchiveMuxer
from ampel.ztf.ingest.ZiCompilerOptions import ZiCompilerOptions
from ampel.ztf.ingest.ZiDataPointShaper import ZiDataPointShaperBase


def _make_muxer(context: AmpelContext, model: UnitModel) -> ZiArchiveMuxer:
    run_id = 0
    logger = AmpelLogger.get_logger()
    updates_buffer = DBUpdatesBuffer(context.db, run_id=run_id, logger=logger)

    muxer = context.loader.new_context_unit(
        model=model,
        context=context,
        logger=logger,
        updates_buffer=updates_buffer,
    )

    return muxer


def get_supplier(loader):
    supplier = ZiAlertSupplier(
        deserialize="avro", loader=UnitTestAlertSupplier(alerts=list(loader))
    )
    return supplier


@pytest.fixture
def raw_alert_dicts(avro_packets):
    def gen():
        for f in avro_packets():
            yield next(fastavro.reader(f))

    return gen


@pytest.fixture
def alerts(raw_alert_dicts):
    def gen():
        for d in raw_alert_dicts():
            yield ZiAlertSupplier.shape_alert_dict(d)

    return gen


@pytest.fixture
def superseded_alerts(superseded_packets):
    def gen():
        for f in superseded_packets():
            yield ZiAlertSupplier.shape_alert_dict(next(fastavro.reader(f)))

    return gen


@pytest.fixture()
def consolidated_alert(raw_alert_dicts):
    """
    Make one mega-alert containing all photopoints for an object, similar to
    the one returned by ArchiveDB.get_photopoints_for_object
    """
    candidates = []
    prv_candidates = []
    upper_limits = []
    for alert in itertools.islice(raw_alert_dicts(), 0, 1):
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


@pytest.mark.parametrize(
    "model",
    [
        UnitModel(unit="ZiArchiveMuxer", config={"history_days": 30}),
    ],
)
def test_instantiate(patch_mongo, dev_context: AmpelContext, model):
    _make_muxer(dev_context, model)


@pytest.fixture
def mock_get_photopoints(mocker, consolidated_alert):
    # mock get_photopoints to return first alert
    mocker.patch(
        "ampel.ztf.ingest.ZiArchiveMuxer.ZiArchiveMuxer.get_photopoints",
        return_value=consolidated_alert,
    )


@pytest.fixture
def mock_archive_muxer(patch_mongo, dev_context, mock_get_photopoints):
    ingester = _make_muxer(
        dev_context, UnitModel(unit="ZiArchiveMuxer", config={"history_days": 30})
    )
    return ingester


@pytest.fixture
def t0_ingester(patch_mongo, dev_context):
    run_id = 0
    logger = AmpelLogger.get_logger()
    updates_buffer = DBUpdatesBuffer(dev_context.db, run_id=run_id, logger=logger)
    ingester = MongoT0Ingester(updates_buffer=updates_buffer)
    compiler = T0Compiler(tier=0, run_id=run_id)
    return ingester, compiler


def test_get_earliest_jd(
    t0_ingester: tuple[MongoT0Ingester, T0Compiler], mock_archive_muxer, alerts
):
    """earliest jd is stable under out-of-order ingestion"""

    alert_list = list(alerts())

    ingester, compiler = t0_ingester

    for i in [2, 0, 1]:

        datapoints = ZiDataPointShaperBase().process(
            alert_list[i].dps, stock=alert_list[i].stock_id
        )
        compiler.add(datapoints, channel="EXAMPLE_TNS_MSIP", trace_id=0)
        compiler.commit(ingester, 0)

        assert mock_archive_muxer.get_earliest_jd(
            alert_list[i].stock_id, datapoints
        ) == min(
            dp["jd"] for dp in alert_list[i].pps
        ), "min jd is min jd of last ingested alert"


def get_handler(context, directives) -> ChainedIngestionHandler:
    run_id = 0
    logger = AmpelLogger.get_logger(console={"level": DEBUG})
    updates_buffer = DBUpdatesBuffer(context.db, run_id=run_id, logger=logger)
    return ChainedIngestionHandler(
        context=context,
        logger=logger,
        run_id=0,
        updates_buffer=updates_buffer,
        directives=directives,
        compiler_opts=ZiCompilerOptions(),
        shaper=UnitModel(unit="ZiDataPointShaper"),
        trace_id={},
        tier=0,
    )


def test_integration(patch_mongo, dev_context, mock_get_photopoints, alerts):
    directive = {
        "channel": "EXAMPLE_TNS_MSIP",
        "ingest": {
            "combine": [
                {"unit": "ZiT1Combiner", "state_t2": [{"unit": "DemoLightCurveT2Unit"}]}
            ],
            "mux": {
                "unit": "ZiArchiveMuxer",
                "config": {"history_days": 30},
                "combine": [
                    {
                        "unit": "ZiT1Combiner",
                        "state_t2": [{"unit": "DemoLightCurveT2Unit"}],
                    }
                ],
            },
        },
    }

    handler = get_handler(dev_context, [IngestDirective(**directive)])

    t0 = dev_context.db.get_collection("t0")
    t1 = dev_context.db.get_collection("t1")
    t2 = dev_context.db.get_collection("t2")
    assert t0.count_documents({}) == 0

    alert_list = list(alerts())

    handler.ingest(
        alert_list[1].dps, stock_id=alert_list[1].stock_id, filter_results=[(0, True)]
    )
    handler.updates_buffer.push_updates()

    assert ZiArchiveMuxer.get_photopoints.called_once()

    # note lack of handler.updates_buffer.push_updates() here;
    # ZiAlertContentIngester has to be synchronous to deal with superseded
    # photopoints
    assert t0.count_documents({}) == len(alert_list[1].dps) + len(
        alert_list[0].dps
    ), "datapoints ingested for archival alert"

    assert t1.count_documents({}) == 2, "two compounds produced"
    assert t2.count_documents({}) == 2, "two t2 docs produced"

    assert t2.find_one(
        {"link": t1.find_one({"dps": {"$size": len(alert_list[1].dps)}})["link"]}
    )
    assert t2.find_one(
        {
            "link": t1.find_one(
                {"dps": {"$size": len(alert_list[1].dps) + len(alert_list[0].dps)}}
            )["link"]
        }
    )


@pytest.fixture
def archive_token(mock_context, monkeypatch):
    if not (token := os.environ.get("ARCHIVE_TOKEN")):
        pytest.skip("archive test requires token")
    monkeypatch.setattr(
        mock_context.loader,
        "vault",
        AmpelVault(
            [DictSecretProvider({"ztf/archive/token": token})]
            + mock_context.loader.vault.providers
        ),
    )
    yield token


def test_get_photopoints_from_api(mock_context, archive_token):
    """
    ZiT1ArchivalCompoundIngester can communicate with the archive service
    """
    muxer = _make_muxer(
        mock_context, UnitModel(unit="ZiArchiveMuxer", config={"history_days": 30})
    )
    alert = muxer.get_photopoints("ZTF18abcfcoo", before_jd=2458300)
    assert len(alert["prv_candidates"]) == 10


def test_deduplication(
    dev_context, t0_ingester: tuple[MongoT0Ingester, T0Compiler], alerts
):
    """
    Database gets only one copy of each datapoint
    """

    alert_list = list(itertools.islice(alerts(), 1, None))

    ingester, compiler = t0_ingester

    pps = []
    uls = []
    for alert in alert_list:
        pps += alert.get_tuples("jd", "fid", data="pps")
        uls += alert.get_values("jd", data="uls")
        datapoints = ZiDataPointShaperBase().process(alert.dps, stock=alert.stock_id)
        compiler.add(datapoints, "channychan", 0)

    assert len(set(uls)) < len(uls), "Some upper limits duplicated in alerts"
    assert len(set(pps)) < len(pps), "Some photopoints duplicated in alerts"

    compiler.commit(ingester, 0)
    ingester.updates_buffer.push_updates()

    t0 = dev_context.db.get_collection("t0")
    assert t0.count_documents({"id": {"$gt": 0}}) == len(set(pps))
    assert t0.count_documents({"id": {"$lt": 0}}) == len(set(uls))


@pytest.fixture
def ingestion_handler_with_mongomuxer(mock_context):
    directive = {
        "channel": "EXAMPLE_TNS_MSIP",
        "ingest": {
            "mux": {
                "unit": "ZiMongoMuxer",
                "combine": [
                    {
                        "unit": "ZiT1Combiner",
                    }
                ],
            },
        },
    }

    return get_handler(mock_context, [IngestDirective(**directive)])


def _ingest(handler: ChainedIngestionHandler, alert: PhotoAlert):
    handler.ingest(alert.dps, filter_results=[(0, True)], stock_id=alert.stock_id)
    len(updates := handler.updates_buffer.db_ops["t1"]) == 1
    update = updates[0]
    assert isinstance(update, UpdateOne)
    dps = update._doc["$setOnInsert"]["dps"]
    handler.updates_buffer.push_updates()
    return dps


def test_out_of_order_ingestion(
    mock_context, ingestion_handler_with_mongomuxer, alerts
):
    """
    Returned alert content does not depend on whether photopoints
    were already committed to the database
    """

    alert_list = list(alerts())
    assert alert_list[-1].pps[0]["jd"] > alert_list[-2].pps[0]["jd"]

    in_order = {
        idx: _ingest(ingestion_handler_with_mongomuxer, alert_list[idx])
        for idx in (-3, -1, -2)
    }

    # clean up mutations
    mock_context.db.get_collection("t0").delete_many({})
    mock_context.db.get_collection("t1").delete_many({})
    alert_list = list(alerts())

    out_of_order = {
        idx: _ingest(ingestion_handler_with_mongomuxer, alert_list[idx])
        for idx in (-3, -2, -1)
    }

    for idx in sorted(in_order.keys()):
        assert in_order[idx] == out_of_order[idx]


def test_superseded_candidates_serial(
    mock_context: AmpelContext,
    ingestion_handler_with_mongomuxer: ChainedIngestionHandler,
    superseded_alerts,
):
    """
    Photopoints are superseded by points from newer alerts with the same jd,rcid
    """

    alerts = list(reversed(list(superseded_alerts())))

    assert alerts[0].pps[0]["jd"] == alerts[1].pps[0]["jd"]
    candids = [alert.pps[0]["candid"] for alert in alerts]
    assert candids[0] < candids[1]

    dps = [_ingest(ingestion_handler_with_mongomuxer, alert) for alert in alerts]

    pp_db = mock_context.db.get_collection("t0").find_one(
        {"id": candids[0]},
    )

    assert "SUPERSEDED" in pp_db["tag"], f"{candids[0]} marked as superseded in db"

@concurrent.process
def run_ingester(config, port):
    """
    Run ingester in a subprocess.
    """

    ctx = DevAmpelContext.new(config=AmpelConfig(config, freeze=True))
    ingester = _make_ingester(ctx)

    conn = socket.create_connection(("127.0.0.1", port))

    conn.send(b"hola")
    reply = b""
    while chunk := conn.recv(4096):
        reply += chunk

    alert = pickle.loads(reply)
    candid = alert.pps[0]["candid"]
    print(f"pid {os.getpid()} got alert {candid}")
    dps = ingester.ingest(alert)
    ingester.updates_buffer.push_updates()

    return candid, dps


@pytest.mark.skip(reason="Concurrency test still broken")
@pytest.mark.asyncio
async def test_superseded_candidates_concurrent(
    dev_context, superseded_packets, unused_tcp_port
):
    """
    Photopoints are marked superseded when alerts are ingested simultaneously
    """

    class Distributor:
        """
        Wait for all clients to connect, then deliver messages all at once
        """

        def __init__(self, payloads):
            self.cond = asyncio.Condition()
            self.payloads = payloads

        async def __call__(self, reader, writer):
            data = await reader.read(100)

            payload = self.payloads.pop()

            # block until all payloads are ready to send
            async with self.cond:
                self.cond.notify_all()
            while self.payloads:
                async with self.cond:
                    await self.cond.wait()

            writer.write(payload)
            await writer.drain()

            writer.close()

    alerts = list(reversed(list(get_supplier(superseded_packets()))))
    assert alerts[0].pps[0]["jd"] == alerts[1].pps[0]["jd"]
    candids = [alert.pps[0]["candid"] for alert in alerts]
    assert candids[0] < candids[1]

    assert dev_context.db.get_collection("t0").find_one({}) is None

    messages = [pickle.dumps(alert) for alert in alerts]

    server = await asyncio.start_server(
        Distributor(messages), "127.0.0.1", unused_tcp_port
    )
    serve = asyncio.create_task(server.start_serving())

    try:
        tasks = [
            run_ingester(dev_context.config.get(), unused_tcp_port)
            for _ in range(len(messages))
        ]
        returns = await asyncio.gather(*tasks)
    finally:
        serve.cancel()

    ingester = _make_ingester(dev_context)

    without = lambda d, ignored_keys: {
        k: v for k, v in d.items() if not k in ignored_keys
    }

    for candid, dps in returns:
        alert = alerts[candids.index(candid)]

        # ensure that returned datapoints match the shaped alert content, save
        # for tags, which can't be known from a single alert
        assert [without(dp, {"tag"}) for dp in dps if dp["_id"] > 0] == sorted(
            [
                without(ingester.project(dp), {"tag"})
                for dp in ingester.pp_shaper.process(copy.deepcopy(alert.pps))
            ],
            key=lambda pp: pp["body"]["jd"],
        ), "photopoints match alert content (except tags)"
        assert [without(dp, {"tag"}) for dp in dps if dp["_id"] < 0] == sorted(
            [
                without(ingester.project(dp), {"tag"})
                for dp in ingester.ul_shaper.process(copy.deepcopy(alert.uls))
            ],
            key=lambda pp: pp["body"]["jd"],
        ), "upper limits match alert content (except tags)"

    t0 = dev_context.db.get_collection("t0")

    def assert_superseded(old, new):
        doc = t0.find_one({"_id": old})
        assert (
            "SUPERSEDED" in doc["tag"] and new in doc["newId"]
        ), f"candid {old} superseded by {new}"

    assert_superseded(candids[0], candids[1])
    assert_superseded(candids[1], candids[2])
    assert (
        "SUPERSEDED" not in t0.find_one({"_id": candids[2]})["tag"]
    ), f"candid {candids[2]} not superseded"