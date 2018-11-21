#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/pipeline/t0/run.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, time, uuid, logging
from astropy.time import Time
from ampel.ztf.archive.ArchiveDB import ArchiveDB
from ampel.ztf.pipeline.t0.load.UWAlertLoader import UWAlertLoader
from ampel.ztf.pipeline.t0.ZISetup import ZISetup
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.load.TarAlertLoader import TarAlertLoader
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.channel.ChannelConfigLoader import ChannelConfigLoader

import pkg_resources

def get_required_resources(channels=None):
	units = set()
	for channel in ChannelConfigLoader.load_configurations(channels, 0):
		for source in channel.sources:
			units.add(source.t0Filter.unitId)
	resources = set()
	for unit in units:
		for resource in AmpelUnitLoader.get_class(0, unit).resources:
			resources.add(resource)
	return resources

def split_private_channels(channels=None, skip_channels=set()):
	public = []
	private = []
	for channel in ChannelConfigLoader.load_configurations(channels, 0):
		if channel.channel in skip_channels:
			continue
		for source in channel.sources:
			if source.stream == "ZTFIPAC":
				if source.parameters.get('ZTFPartner', False):
					private.append(channel.channel)
				else:
					public.append(channel.channel)
	
	return public, private

def override_channels(config, channel_files):
	import json
	from ampel.pipeline.config.channel.ChannelConfig import ChannelConfig
	config['channels'].clear()
	for fname in channel_files:
		with open(fname) as f:
			for chan_dict in json.load(f):
				# validate
				ChannelConfig.create(0, **chan_dict)
				if chan_dict['channel'] in config['channels']:
					raise ValueError("Channel {} is already defined".format(chan_dict['channel']))
				config['channels'][chan_dict['channel']] = chan_dict
	return config

def run_alertprocessor():

	# Apparently, this is wished
	AmpelLogger.set_default_stream(sys.stderr)
	log = logging.getLogger('ampel-ztf-alertprocessor')

	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.add_argument('-v', '--verbose', default=False, action="store_true")
	parser.add_argument('--publish-stats', nargs='*', default=['jobs', 'graphite'])
	action = parser.add_mutually_exclusive_group(required=True)
	action.add_argument('--broker', default='epyc.astro.washington.edu:9092')
	action.add_argument('--tarfile', default=None)
	action.add_argument('--archive', nargs=2, type=Time, default=None, metavar='TIME')
	parser.add_argument('--no-update-archive', dest='update_archive',
	    default=True, action='store_false', help="Don't update the archive")
	parser.add_argument('--group', default=uuid.uuid1().hex, help="Kafka consumer group name")
	parser.add_argument('--timeout', default=3600, type=int, help='Kafka timeout')
	parser.add_argument('--statistics-interval', default=0, type=int, help='Report Kafka statistics to Graphite')
	action = parser.add_mutually_exclusive_group(required=False)
	action.add_argument('--channels', default=None, nargs="+", 
		help="Run only these filters on all ZTF alerts")
	action.add_argument('--private', default=None, action="store_true", 
		help="Run partnership filters on all ZTF alerts")
	action.add_argument('--public', dest="private", default=None, action="store_false", 
		help="Run public filters on public ZTF alerts only")
	parser.add_argument('--skip-channels', default=[], nargs="+", 
		help="Do not run these filters")
	parser.add_argument('--override-channels', default=None, nargs="+",
		help="Take channel definitions from these JSON files")
	
	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	if 'graphite' in opts.publish_stats:
		parser.require_resource('graphite')
	if opts.archive:
		parser.require_resource('archive', ['reader'])
	elif not opts.tarfile:
		parser.require_resource('archive', ['writer'])
	# flesh out parser with resources required by t0 units
	AmpelConfig.set_config(opts.config)
	parser.require_resources(*get_required_resources(opts.channels))
	# parse again
	opts = parser.parse_args()

	if opts.override_channels is not None:
		AmpelConfig.set_config(override_channels(opts.config, opts.override_channels))

	partnership = True
	if opts.private is not None:
		public, private = split_private_channels(skip_channels=opts.skip_channels)
		if opts.private:
			channels = private
			opts.group += "-partnership"
		else:
			channels = public
			opts.group += "-public"
			partnership = False
	elif opts.channels is None:
		channels = None
	else:
		channels = set(opts.channels) - set(opts.skip_channels)
	log.info('Running with channels {}'.format(channels))

	count = 0
	#AlertProcessor.iter_max = 100
	alert_processed = AlertProcessor.iter_max
	archive = None

	if opts.tarfile is not None:
		infile = opts.tarfile
		loader = TarAlertLoader(tar_path=opts.tarfile)

	elif opts.archive is not None:
		# Quieten sqlalchemy logger
		logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

		infile = 'archive'
		archive = ArchiveDB(
			AmpelConfig.get_config('resources.archive.reader')
		)
		loader = archive.get_alerts_in_time_range(
			opts.archive[0].jd, 
			opts.archive[1].jd,
			programid=(None if partnership else 1),
			group_name=opts.group,
			block_size=5000
		)

	else:
		# insert loaded alerts into the archive only if 
		# they didn't come from the archive in the first place
		infile = '{} group {}'.format(opts.broker, opts.group)
		# Quieten sqlalchemy logger
		logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

		loader = iter(UWAlertLoader(
			bootstrap=opts.broker, 
			group_name=opts.group, 
			partnership=partnership,
			update_archive=opts.update_archive,
			statistics_interval=0 if not 'graphite' in opts.publish_stats else opts.statistics_interval,
			timeout=opts.timeout
		))

	processor = AlertProcessor(
		ZISetup(serialization="avro" if opts.tarfile else None), 
		publish_stats=opts.publish_stats, 
		channels=channels
	)

	while alert_processed == AlertProcessor.iter_max:

		t0 = time.time()
		log.info('Running on {}'.format(infile))
		try:
			alert_processed = processor.run(loader, full_console_logging=opts.verbose)
		finally:
			dt = time.time()-t0
			log.info(
				'({}) {} alerts in {:.1f}s; {:.1f}/s'.format(
					infile, alert_processed, dt, alert_processed/dt
				)
			)
