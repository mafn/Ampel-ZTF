
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

@pytest.mark.integration
@pytest.mark.parametrize("config_source,alert_source", [("env", "tarball"), ("env", "archive"), ("cmdline", "tarball"), ("cmdline", "archive"), ("cmdline", "kafka")])
def test_alertprocessor_entrypoint(alert_tarball, alert_generator, empty_mongod, empty_archive, graphite, kafka_stream, config_source, alert_source):
	from ampel.ztf.archive.ArchiveDB import ArchiveDB
	if alert_source == "tarball":
		cmd = ['ampel-ztf-alertprocessor', '--tarfile', alert_tarball, '--channels', 'HU_RANDOM']

	elif alert_source == "archive":
		populate_archive(alert_generator, empty_archive)
		db = ArchiveDB(empty_archive)
		assert db.count_alerts() > 0
		del db
		cmd = ['ampel-ztf-alertprocessor', '--archive', '2000-01-01', '2099-01-01', '--channels', 'HU_RANDOM']
	elif alert_source == "kafka":
		db = ArchiveDB(empty_archive)
		assert db.count_alerts() == 0
		del db
		cmd = ['ampel-ztf-alertprocessor', '--broker', kafka_stream, '--timeout=10', '--channels', 'HU_RANDOM']
		
	if config_source == "env":
		env = {**resource_env(empty_mongod, 'mongo', 'writer'),
		       **resource_env(empty_archive, 'archive', "writer" if alert_source == "kafka" else "reader"),
		       **resource_env(graphite, 'graphite'),
		       'SLOT': '1'}
		env.update(os.environ)
	elif config_source == "cmdline":
		env = os.environ
		cmd += resource_args(empty_mongod, 'mongo', 'writer') \
		    + resource_args(graphite, 'graphite')
		if alert_source != "tarball":
			cmd += resource_args(empty_archive, 'archive', "writer" if alert_source == "kafka" else "reader")
	subprocess.check_call(cmd, env=env)
	from pymongo import MongoClient
	mc = MongoClient(empty_mongod)
	assert mc['Ampel_logs']['troubles'].count({}) == 0
	if alert_source == "kafka":
		db = ArchiveDB(empty_archive)
		assert db.count_alerts() == 30

@pytest.mark.skip
@pytest.mark.integration
def test_run_all_tiers(alert_tarball, empty_mongod, empty_archive, graphite):
	from pymongo import MongoClient
	cmd = ['ampel-ztf-alertprocessor', '--tarfile', alert_tarball]
	env = os.environ
	cmd += resource_args(empty_mongod, 'mongo', 'writer') \
	    + resource_args(graphite, 'graphite')
	subprocess.check_call(cmd, env=env)
	assert MongoClient(empty_mongod)['Ampel_logs']['troubles'].count({}) == 0

	cmd = ['ampel-t2', '--interval', '-1']
	subprocess.check_call(cmd, env=env)
	assert MongoClient(empty_mongod)['Ampel_logs']['troubles'].count({}) == 0

	cmd = ['ampel-t3', 'dryrun']
	subprocess.check_call(cmd, env=env)

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
	
	public, private = split_private_channels()
	assert len(private) > len(public)
	assert len(public) > 0

def test_required_resources(live_config):
	from ampel.ztf.pipeline.t0.run import get_required_resources
	from ampel.pipeline.config.ConfigLoader import ConfigLoader
	
	resources = get_required_resources()
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

@pytest.fixture
def troubled_alert(alert_generator, minimal_ingestion_config, mocker):
	from ampel.pipeline.db.AmpelDB import AmpelDB
	from ampel.pipeline.t0.AlertProcessor import AlertProcessor
	from ampel.ztf.pipeline.t0.ZISetup import ZISetup
	import itertools

	# raise an exception from a filter unit
	# pytest-mock's mocker fixture does not actually work as a context manager
	try:
		mocker.patch('ampel.pipeline.t0.filter.BasicFilter.BasicFilter.apply', side_effect=ValueError)
		ap = AlertProcessor(ZISetup(serialization=None), publish_stats=[])
		ap.run(itertools.islice(alert_generator(), 1), full_console_logging=True, raise_exc=False)
	finally:
		mocker.stopall()
	assert AmpelDB.get_collection('troubles').count_documents({}) == 2, "exceptions logged for each channel"

def test_ingestion_from_troubles(alert_generator, minimal_ingestion_config, troubled_alert):
	"""
	Recover alerts where a filter raised an error
	"""
	from ampel.pipeline.t0.AlertProcessor import AlertProcessor
	from ampel.ztf.pipeline.t0.ZISetup import ZISetup
	from ampel.ztf.pipeline.t0.TroublesSetup import TroublesSetup
	from ampel.ztf.pipeline.t0.load.TroublesAlertLoader import TroublesAlertLoader
	from ampel.ztf.pipeline.t0.load.TroublesAlertShaper import TroublesAlertShaper
	from ampel.pipeline.db.AmpelDB import AmpelDB
	from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader
	import types
	import itertools
	from datetime import datetime, timedelta

	assert len(list(TroublesAlertLoader.alerts(remove_records=False))) == 1, "reports are coalesced by alert id"
	assert len(list(TroublesAlertLoader.alerts(remove_records=False, after=datetime.now()+timedelta(days=1)))) == 0, "reports are filtered by time"
	assert len(list(TroublesAlertLoader.alerts(remove_records=False, after=datetime.now()-timedelta(days=1)))) == 1, "reports are filtered by time"

	alert_content = next(ZISetup(serialization=None).get_alert_supplier(alert_generator()))
	reco_content = TroublesAlertShaper.shape(next(TroublesAlertLoader().alerts()))
	assert reco_content == alert_content, "original alert content recovered"

	# run it again
	ap = AlertProcessor(TroublesSetup(serialization=None), publish_stats=[])
	assert ap.run(TroublesAlertLoader.alerts(limit=1), full_console_logging=True, raise_exc=False) == 1, "1 alert reprocessed"
	assert AmpelDB.get_collection('troubles').count_documents({}) == 0, "no further exceptions"

def test_entrypoint_from_troubles(troubled_alert, minimal_ingestion_config):
	import subprocess
	from astropy.time import Time
	from ampel.pipeline.db.AmpelDB import AmpelDB
	from ampel.pipeline.config.ConfigLoader import ConfigLoader
	import tempfile
	import json

	with tempfile.NamedTemporaryFile(mode='w+') as f:
		conf = {
			'resources': minimal_ingestion_config['resources'],
			'channels': minimal_ingestion_config['channels'],
		}
		json.dump(conf, f)
		f.flush()
		assert ConfigLoader.load_config(f.name, gather_plugins=False), "config is valid"
		cmd = ['ampel-ztf-alertprocessor', '-c', f.name, '--troubles', (Time.now()-1).isot, '--publish-stats', 'none', '--channels', '0', '1']
		subprocess.check_call(cmd)

	assert AmpelDB.get_collection('troubles').count_documents({}) == 0, "no further exceptions"
