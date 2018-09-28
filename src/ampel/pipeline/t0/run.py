import os, time, uuid, logging
from astropy.time import Time
from ampel.archive.ArchiveDB import ArchiveDB
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.load.ZIAlertShaper import ZIAlertShaper
from ampel.pipeline.t0.load.TarAlertLoader import TarAlertLoader
from ampel.pipeline.t0.load.UWAlertLoader import UWAlertLoader
from ampel.pipeline.t0.load.AlertSupplier import AlertSupplier
from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ChannelLoader import ChannelLoader
from ampel.pipeline.t0.ZIStreamSetup import ZIStreamSetup


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
	loader = ChannelLoader(source="ZTFIPAC", tier=0)
	parser.require_resources(*loader.get_required_resources())
	# parse again
	opts = parser.parse_args()

	partnership = True
	if opts.private is not None:
		params = loader.get_source_parameters()
		private = {k for k,v in params.items() if v.get('ZTFPartner', False)}
		if opts.private:
			channels = private
			opts.group += "-partnership"
		else:
			channels = set(params.keys()).difference(private)
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
		infile = '{} group {}'.format(opts.broker, opts.group)
		loader = UWAlertLoader(
			opts.broker, 
			group_name=opts.group, 
			partnership=partnership, 
            update_archive=True,
			timeout=3600
		)

	processor = AlertProcessor(
		# insert loaded alerts into the archive only if 
		# they didn't come from the archive in the first place
		ZIStreamSetup(), 
		publish_stats=["jobs", "graphite"], 
		channels=channels
	)

	log = logging.getLogger('ampel-alertprocessor')

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
