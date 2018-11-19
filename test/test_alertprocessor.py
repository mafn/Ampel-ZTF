
import pytest
import subprocess
import os
from urllib.parse import urlparse

def resource_args(uri, name, role=None):
	uri = urlparse(uri)
	prefix = '--{}'.format(name)
	args = [prefix+'-host', uri.hostname, prefix+'-port', str(uri.port)]
	if role is not None:
		if uri.username is not None:
			args += [prefix+'-'+role+'-username', uri.username]
		if uri.password is not None:
			args += [prefix+'-'+role+'-password', uri.password]
	return args

def resource_env(uri, name, role=None):
	uri = urlparse(uri)
	prefix = name.upper() + "_"
	env = {prefix+"HOSTNAME": uri.hostname, prefix+"PORT": str(uri.port)}
	if role is not None:
		if uri.username is not None:
			env[prefix+role.upper()+"_USERNAME"] = uri.username
		if uri.password is not None:
			env[prefix+role.upper()+"_PASSWORD"] = uri.password
	return env

@pytest.fixture
def empty_mongod(mongod):
	from pymongo import MongoClient
	mc = MongoClient(mongod)
	mc.drop_database('Ampel_logs')
	yield mongod
	mc.drop_database('Ampel_logs')

def populate_archive(alert_generator, empty_archive):
	from itertools import islice
	from ampel.ztf.pipeline.t0.ArchiveUpdater import ArchiveUpdater

	updater = ArchiveUpdater(empty_archive)
	for idx, (alert, schema) in enumerate(islice(alert_generator(with_schema=True), 100)):
		updater.insert_alert(alert, schema, idx%16, 0)
	return idx+1

@pytest.mark.parametrize("config_source,alert_source", [("env", "tarball"), ("env", "archive"), ("cmdline", "tarball"), ("cmdline", "archive"), ("cmdline", "kafka")])
def test_alertprocessor_entrypoint(alert_tarball, alert_generator, empty_mongod, empty_archive, graphite, kafka_stream, config_source, alert_source):
	from ampel.ztf.archive.ArchiveDB import ArchiveDB
	if alert_source == "tarball":
		cmd = ['ampel-ztf-alertprocessor', '--tarfile', alert_tarball, '--channels', 'HU_RANDOM']

	elif alert_source == "archive":
		populate_archive(alert_generator, empty_archive)
		db = ArchiveDB(empty_archive)
		assert db.get_statistics()['alert']['rows'] > 0
		del db
		cmd = ['ampel-ztf-alertprocessor', '--archive', '2000-01-01', '2099-01-01', '--channels', 'HU_RANDOM']
	elif alert_source == "kafka":
		db = ArchiveDB(empty_archive)
		assert db.get_statistics()['alert']['rows'] == 0
		del db
		cmd = ['ampel-ztf-alertprocessor', '--broker', kafka_stream, '--timeout=10', '--channels', 'HU_RANDOM']
		
	if config_source == "env":
		env = {**resource_env(empty_mongod, 'mongo', 'writer'),
		       **resource_env(empty_archive, "writer" if alert_source == "kafka" else "reader"),
		       **resource_env(graphite, 'graphite'),
		       'SLOT': '1'}
		env.update(os.environ)
	elif config_source == "cmdline":
		env = os.environ
		cmd += resource_args(empty_mongod, 'mongo', 'writer') \
		    + resource_args(empty_archive, 'archive', "writer" if alert_source == "kafka" else "reader") \
		    + resource_args(graphite, 'graphite')
	subprocess.check_call(cmd, env=env)
	from pymongo import MongoClient
	mc = MongoClient(empty_mongod)
	assert mc['Ampel_logs']['troubles'].count({}) == 0
	if alert_source == "kafka":
		db = ArchiveDB(empty_archive)
		assert db.get_statistics()['alert']['rows'] == 30

def test_kafka_stream(kafka_stream):
	"""Does the Kafka stream itself work?"""
	from ampel.ztf.pipeline.t0.load.UWAlertLoader import UWAlertLoader
	
	loader = UWAlertLoader(partnership=True, bootstrap=kafka_stream, timeout=10)
	count = 0
	for alert in loader:
		count += 1
		if count == 30:
			break
	assert count == 30

@pytest.fixture
def live_config():
	from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
	from ampel.pipeline.config.AmpelConfig import AmpelConfig
	AmpelConfig.reset()
	AmpelArgumentParser().parse_args(args=[])
	yield
	AmpelConfig.reset()

def test_private_channel_split(live_config):
	from ampel.ztf.pipeline.t0.run import split_private_channels
	from ampel.pipeline.config.ConfigLoader import ConfigLoader
	
	config = ConfigLoader.load_config()
	public, private = split_private_channels(config)
	assert len(public) == 1

def test_required_resources():
	from ampel.ztf.pipeline.t0.run import get_required_resources
	from ampel.pipeline.config.ConfigLoader import ConfigLoader
	
	config = ConfigLoader.load_config()
	resources = get_required_resources(config)
	assert len(resources) > 0

def test_setup():
	from ampel.ztf.pipeline.t0.ZISetup import ZISetup
	ZISetup()

def test_ingestion_from_archive(empty_archive, alert_generator, minimal_ingestion_config):
	from ampel.ztf.archive.ArchiveDB import ArchiveDB
	from ampel.pipeline.t0.AlertProcessor import AlertProcessor
	from ampel.ztf.pipeline.t0.ZISetup import ZISetup

	count = populate_archive(alert_generator, empty_archive)

	db = ArchiveDB(empty_archive)
	alerts = db.get_alerts_in_time_range(-float('inf'), float('inf'), programid=1)

	ap = AlertProcessor(ZISetup(serialization=None), publish_stats=[])
	iter_count = ap.run(alerts)
	assert iter_count == count
