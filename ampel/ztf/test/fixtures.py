
from os.path import abspath, join, dirname
import pytest

@pytest.fixture(scope='session')
def alert_tarball():
	return join(dirname(__file__), 'test-data', 'ztf_public_20180819_mod1000.tar.gz')

@pytest.fixture(scope='session', params=['ztf_20200106_programid2_zuds.tar.gz', 'ztf_20200106_programid2_zuds_stack.tar.gz'])
def zuds_alert_generator(request):
	import itertools
	import fastavro
	from ampel.pipeline.t0.load.TarballWalker import TarballWalker
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
	from ampel.t0.load.TarballWalker import TarballWalker
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
	from ampel.ztf.utils.ZIAlertUtils import ZIAlertUtils
	def lightcurves():
		for alert in alert_generator():
			lightcurve = ZIAlertUtils.to_lightcurve(content=alert)
			assert isinstance(lightcurve.get_photopoints(), tuple)
			yield lightcurve

	return lightcurves