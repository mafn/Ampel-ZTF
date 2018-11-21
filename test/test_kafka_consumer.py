
import pytest
import socket

@pytest.fixture
def kafka_broker():
	if socket.gethostname().split('.')[0] not in {'burst', 'transit', 'ztf-wgs'}:
		pytest.skip("Can't connect to UW Kafka from here")
	return 'epyc.astro.washington.edu:9092'

def test_stats(kafka_broker, mocker):
	import time
	from unittest.mock import MagicMock
	from ampel.pipeline.common.AmpelUtils import AmpelUtils
	from ampel.ztf.pipeline.t0.load.UWAlertLoader import UWAlertLoader

	feeder = mocker.patch('ampel.pipeline.common.GraphiteFeeder.GraphiteFeeder')
	get_config = mocker.patch('ampel.pipeline.config.AmpelConfig.AmpelConfig.get_config')
	get_config.return_value = None
	loader = UWAlertLoader(kafka_broker, statistics_interval=1, timeout=100)

	t0 = time.time()
	alerts = loader.alerts()

	def got_stats():
		loader.graphite.add_stats.called and len(AmpelUtils.flatten_dict(loader.graphite.add_stats.call_args_list[-1][0][0])) > 0
	while time.time() - t0 < 10 and not got_stats():
		next(alerts)

	assert loader.graphite.send.called
	assert loader.graphite.add_stats.called
	flattened = AmpelUtils.flatten_dict(loader.graphite.add_stats.call_args_list[-1][0][0])
	assert len(flattened) > 0
