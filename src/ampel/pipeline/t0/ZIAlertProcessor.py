import os, time, uuid, logging
from astropy.time import Time
from ampel.archive.ArchiveDB import ArchiveDB
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.load.ZIAlertShaper import ZIAlertShaper
from ampel.pipeline.t0.load.TarAlertLoader import TarAlertLoader
from ampel.pipeline.t0.load.UWAlertLoader import UWAlertLoader
from ampel.pipeline.t0.load.AlertSupplier import AlertSupplier
from ampel.pipeline.config.ArgumentParser import AmpelArgumentParser
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.channel.ChannelConfig import ChannelConfig
from ampel.pipeline.t0.ZISetup import ZISetup
import pkg_resources

def get_required_resources(config, channels=None):
	units = set()
	for name, channel_config in config['channels'].items():
		channel = ChannelConfig.parse_obj(channel_config)
		if not channel.active or (channels is not None and name not in channels):
			continue
		for source in channel.sources:
			units.add(source.t0Filter.unitId)
	resources = set()
	for filter_id in units:
		filter_entry_point = next(
			pkg_resources.iter_entry_points('ampel.pipeline.t0', filter_id), 
			None
		)
		if filter_entry_point is None:
			raise ValueError("Unknown unit {}".format(filter_id))
		unit = filter_entry_point.resolve()
		for resource in unit.resources:
			resources.add(resource)
	return resources

def split_private_channels(config, channels=None):
	public = []
	private = []
	for name, channel_config in config['channels'].items():
		channel = ChannelConfig.parse_obj(channel_config)
		if not channel.active or (channels is not None and name not in channels):
			continue
		for source in channel.sources:
			if source.stream == "ZTFIPAC":
				if source.parameters.get('ZTFPartner', False):
					private.append(name)
				else:
					public.append(name)
	
	return public, private

def run_alertprocessor():

	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.require_resource('archive', ['writer'])
	parser.require_resource('graphite')
	action = parser.add_mutually_exclusive_group(required=True)
	action.add_argument('--broker', default='epyc.astro.washington.edu:9092')
	action.add_argument('--tarfile', default=None)
	action.add_argument('--archive', nargs=2, type=Time, default=None, metavar='TIME')
	parser.add_argument('--slot', env_var='SLOT', type=int, 
		default=None, help="Index of archive reader worker")
	parser.add_argument('--group', default=uuid.uuid1().hex, help="Kafka consumer group name")
	action = parser.add_mutually_exclusive_group(required=False)
	action.add_argument('--channels', default=None, nargs="+", 
		help="Run only these filters on all ZTF alerts")
	action.add_argument('--private', default=None, action="store_true", 
		help="Run partnership filters on all ZTF alerts")
	action.add_argument('--public', dest="private", default=None, action="store_false", 
		help="Run public filters on public ZTF alerts only")
	
	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t0 units
	parser.require_resources(*get_required_resources(opts.config, opts.channels))
	# parse again
	opts = parser.parse_args()

	partnership = True
	if opts.private is not None:
		public, private = split_private_channels(opts.config)
		if opts.private:
			channels = private
			opts.group += "-partnership"
		else:
			channels = public
			opts.group += "-public"
			partnership = False
	else:
		channels = opts.channels

	count = 0
	#AlertProcessor.iter_max = 100
	alert_processed = AlertProcessor.iter_max
	archive = None

	if opts.tarfile is not None:
		infile = opts.tarfile
		loader = TarAlertLoader(tar_path=opts.tarfile)

	elif opts.archive is not None:
		if opts.slot is None:
			import os
			print(os.environ)
			parser.error("You must specify --slot in archive mode")
		elif opts.slot < 1 or opts.slot > 16:
			parser.error("Slot number must be between 1 and 16 (got {})".format(opts.slot))

		infile = 'archive'
		archive = ArchiveDB(
			AmpelConfig.get_config('resources.archive.writer')
		)
		loader = archive.get_alerts_in_time_range(
			opts.archive[0].jd, 
			opts.archive[1].jd,
			partitions=(opts.slot-1), 
			programid=(None if partnership else 1)
		)

	else:
		# insert loaded alerts into the archive only if 
		# they didn't come from the archive in the first place
		infile = '{} group {}'.format(opts.broker, opts.group)
		loader = UWAlertLoader(
			opts.broker, 
			group_name=opts.group, 
			partnership=partnership, 
            update_archive=True,
			timeout=3600
		)

	processor = AlertProcessor(
		ZISetup(serialization=None if opts.archive else "avro"), 
		publish_stats=["jobs", "graphite"], 
		channels=channels
	)

	log = logging.getLogger('ampel-ztf-alertprocessor')

	while alert_processed == AlertProcessor.iter_max:

		t0 = time.time()
		log.info('Running on {}'.format(infile))
		try:
			alert_processed = processor.run(loader, full_console_logging=False)
		finally:
			dt = time.time()-t0
			log.info(
				'({}) {} alerts in {:.1f}s; {:.1f}/s'.format(
					infile, alert_processed, dt, alert_processed/dt
				)
			)
