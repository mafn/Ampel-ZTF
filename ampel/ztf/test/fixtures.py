from functools import partial
from os import environ
from os.path import dirname, join
from pathlib import Path

import mongomock
import pymongo
import pytest
import yaml

from ampel.alert.load.TarAlertLoader import TarAlertLoader
from ampel.config.AmpelConfig import AmpelConfig
from ampel.dev.DevAmpelContext import DevAmpelContext


@pytest.fixture(scope="session")
def archive():
    if "ARCHIVE_HOSTNAME" in environ and "ARCHIVE_PORT" in environ:
        yield "postgresql://ampel@{}:{}/ztfarchive".format(
            environ["ARCHIVE_HOSTNAME"], environ["ARCHIVE_PORT"]
        )
    else:
        pytest.skip("Requires a Postgres database")


@pytest.fixture
def empty_archive(archive):
    """
    Yield archive database, dropping all rows when finished
    """
    from sqlalchemy import create_engine, MetaData

    engine = create_engine(archive)
    meta = MetaData()
    meta.reflect(bind=engine)
    try:
        with engine.connect() as connection:
            for name, table in meta.tables.items():
                if name != "versions":
                    connection.execute(table.delete())
        yield archive
    finally:
        with engine.connect() as connection:
            for name, table in meta.tables.items():
                if name != "versions":
                    connection.execute(table.delete())


@pytest.fixture(scope="session")
def kafka():
    if "KAFKA_HOSTNAME" in environ and "KAFKA_PORT" in environ:
        yield "{}:{}".format(environ["KAFKA_HOSTNAME"], environ["KAFKA_PORT"])
    else:
        pytest.skip("Requires a Kafka instance")


@pytest.fixture(scope="session")
def kafka_stream(kafka, alert_tarball):
    import itertools

    from confluent_kafka import Producer

    from ampel.alert.load.TarballWalker import TarballWalker

    atat = TarballWalker(alert_tarball)
    print(kafka)
    producer = Producer({"bootstrap.servers": kafka})
    for i, fileobj in enumerate(itertools.islice(atat.get_files(), 0, 1000, 1)):
        producer.produce("ztf_20180819_programid1", fileobj.read())
        print(f"sent {i}")
    producer.flush()
    yield kafka


@pytest.fixture(scope="session")
def alert_tarball():
    return join(dirname(__file__), "test-data", "ztf_public_20180819_mod1000.tar.gz")


@pytest.fixture(
    scope="session",
    params=[
        "ztf_20200106_programid2_zuds.tar.gz",
        "ztf_20200106_programid2_zuds_stack.tar.gz",
    ],
)
def zuds_alert_generator(request):
    import itertools

    import fastavro

    from ampel.alert.load.TarballWalker import TarballWalker

    def alerts(with_schema=False):
        candids = set()
        try:
            atat = TarballWalker(join(dirname(__file__), "test-data", request.param))
            for fileobj in itertools.islice(atat.get_files(), 0, 1000, 1):
                reader = fastavro.reader(fileobj)
                alert = next(reader)
                if alert["candid"] in candids:
                    continue
                else:
                    candids.add(alert["candid"])
                if with_schema:
                    yield alert, reader.writer_schema
                else:
                    yield alert
        except FileNotFoundError:
            raise pytest.skip("{} does not exist".format(request.param))

    return alerts


@pytest.fixture(scope="session")
def alert_generator(alert_tarball):
    import itertools

    import fastavro

    from ampel.alert.load.TarballWalker import TarballWalker

    def alerts(with_schema=False):
        atat = TarballWalker(alert_tarball)
        for fileobj in itertools.islice(atat.get_files(), 0, 1000, 1):
            reader = fastavro.reader(fileobj)
            alert = next(reader)
            if with_schema:
                yield alert, reader.writer_schema
            else:
                yield alert

    return alerts


@pytest.fixture(scope="session")
def lightcurve_generator(alert_generator):
    from ampel.ztf.dev.ZTFAlert import ZTFAlert

    def lightcurves():
        for alert in alert_generator():
            lightcurve = ZTFAlert.to_lightcurve(content=alert)
            assert isinstance(lightcurve.get_photopoints(), tuple)
            yield lightcurve

    return lightcurves


@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.db.AmpelDB.MongoClient", mongomock.MongoClient)


@pytest.fixture
def dev_context():
    config = AmpelConfig.load(
        Path(__file__).parent / "test-data" / "testing-config.yaml",
    )
    custom_conf = {}
    if "MONGO_HOSTNAME" in environ:
        custom_conf[
            "resource.mongo"
        ] = f"mongodb://{environ['MONGO_HOSTNAME']}:{environ.get('MONGO_PORT', 27017)}"
    try:
        return DevAmpelContext.new(
            config=config, purge_db=True, custom_conf=custom_conf
        )
    except pymongo.errors.ServerSelectionTimeoutError:
        raise pytest.skip(
            f"No mongod listening on {(custom_conf or config).get('resource.mongo')}"
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
    Three alerts, received within 100 ms, with the same points but different candids
    """
    return partial(
        TarAlertLoader, Path(__file__).parent / "test-data" / "ZTF18acruwxq.tar.gz"
    )


@pytest.fixture
def first_pass_config():
    with open(Path(__file__).parent / "test-data" / "testing-config.yaml") as f:
        return yaml.safe_load(f)
