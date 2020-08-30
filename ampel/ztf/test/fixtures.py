
from os.path import abspath, join, dirname
import pytest

from os import environ

@pytest.fixture(scope="session")
def archive():
	if 'ARCHIVE_HOSTNAME' in environ and 'ARCHIVE_PORT' in environ:
		yield 'postgresql://ampel@{}:{}/ztfarchive'.format(environ['ARCHIVE_HOSTNAME'], environ['ARCHIVE_PORT'])
	else:
		pytest.skip("Requires a Postgres database")

@pytest.fixture
def empty_archive(archive):
	"""
	Yield archive database, dropping all rows when finished
	"""
	from sqlalchemy import select, create_engine, MetaData

	engine = create_engine(archive)
	meta = MetaData()
	meta.reflect(bind=engine)
	try:
		with engine.connect() as connection:
			for name, table in meta.tables.items():
				if name != 'versions':
					connection.execute(table.delete())
		yield archive
	finally:
		with engine.connect() as connection:
			for name, table in meta.tables.items():
				if name != 'versions':
					connection.execute(table.delete())


@pytest.fixture(scope="session")
def kafka():
	if 'KAFKA_HOSTNAME' in environ and 'KAFKA_PORT' in environ:
		yield '{}:{}'.format(environ['KAFKA_HOSTNAME'], environ['KAFKA_PORT'])
	else:
		pytest.skip("Requires a Kafka instance")

@pytest.fixture(scope="session")
def kafka_stream(kafka, alert_tarball):
	import itertools
	from confluent_kafka import Producer
	from ampel.alert.load.TarballWalker import TarballWalker
	atat = TarballWalker(alert_tarball)
	print(kafka)
	producer = Producer({'bootstrap.servers': kafka})
	for i,fileobj in enumerate(itertools.islice(atat.get_files(), 0, 1000, 1)):
		producer.produce('ztf_20180819_programid1', fileobj.read())
		print(f'sent {i}')
	producer.flush()
	yield kafka

@pytest.fixture(scope='session')
def alert_tarball():
	return join(dirname(__file__), 'test-data', 'ztf_public_20180819_mod1000.tar.gz')

@pytest.fixture(scope='session', params=['ztf_20200106_programid2_zuds.tar.gz', 'ztf_20200106_programid2_zuds_stack.tar.gz'])
def zuds_alert_generator(request):
	import itertools
	import fastavro
	from ampel.alert.load.TarballWalker import TarballWalker
	def alerts(with_schema=False):
		candids = set()
		try:
			atat = TarballWalker(join(dirname(__file__), 'test-data', request.param))
			for fileobj in itertools.islice(atat.get_files(), 0, 1000, 1):
				reader = fastavro.reader(fileobj)
				alert = next(reader)
				if alert['candid'] in candids:
					continue
				else:
					candids.add(alert['candid'])
				if with_schema:
					yield alert, reader.writer_schema
				else:
					yield alert
		except FileNotFoundError:
			raise pytest.skip("{} does not exist".format(request.param))
	return alerts

@pytest.fixture(scope='session')
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

@pytest.fixture(scope='session')
def lightcurve_generator(alert_generator):
	from ampel.ztf.dev.ZTFAlert import ZTFAlert
	def lightcurves():
		for alert in alert_generator():
			lightcurve = ZTFAlert.to_lightcurve(content=alert)
			assert isinstance(lightcurve.get_photopoints(), tuple)
			yield lightcurve

	return lightcurves