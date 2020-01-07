#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/pipeline/t0/load/UWAlertLoader.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import io, time, itertools, logging, uuid, fastavro
import json
from collections import defaultdict
from ampel.ztf.pipeline.t0.load.AllConsumingConsumer import AllConsumingConsumer

log = logging.getLogger(__name__)

class UWAlertLoader:
	"""
	Iterable class that loads avro alerts from the Kafka stream 
	provided by University of Washington (UW) 
	"""

	def __init__(self, 
		partnership,
		bootstrap='partnership.alerts.ztf.uw.edu:9092', 
		group_name=uuid.uuid1(), 
		update_archive=False,
		statistics_interval=0,
		timeout=1
	):
		"""
		:param bool partnership: if True, subscribe to ZTF partnership alerts. Otherwise,
	    subscribe only to the public alert stream
		:param str bootstrap: host:port of Kafka server
		:param bytes group_name: consumer group name. Fetchers with the same group name
	    will be load balanced and receive disjoint sets of messages
		:param bool update_archive: if True, fetched alerts will be inserted into 
		the archive db using ampel.ztf.pipeline.t0.ArchiveUpdater
		:param int timeout: time to wait for messages before giving up, in seconds
		"""
		topics = ['^ztf_.*_programid1$', '^ztf_.*_programid3_public$']

		if partnership:
			topics.append('^ztf_.*_programid2$')
		config = {'group.id':group_name}

		if update_archive:
			from ampel.pipeline.config.AmpelConfig import AmpelConfig
			from ampel.ztf.pipeline.t0.ArchiveUpdater import ArchiveUpdater
			self.archive_updater = ArchiveUpdater(
				AmpelConfig.get_config('resources.archive.writer')
			)
		else:
			self.archive_updater = None

		if statistics_interval > 0:
			from ampel.pipeline.config.AmpelConfig import AmpelConfig
			from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
			self.graphite = GraphiteFeeder(
				AmpelConfig.get_config('resources.graphite.default'),
				autoreconnect = True
			)
			config['stats_cb'] = self.report_statistics
			config['statistics.interval.ms'] = 1000*statistics_interval
		else:
			self.graphite = None

		self._consumer = AllConsumingConsumer(
			bootstrap, timeout=timeout, topics=topics, **config
		)

	def report_statistics(self, payload):
		if self.graphite is None:
			return
		try:
			stats = json.loads(payload)
			offsets = {
				topic: sum(p['hi_offset']-p['lo_offset'] for p in topic_data['partitions'].values() if p.get('hi_offset',-1) >= 0)
				for topic, topic_data in stats['topics'].items()
			}
			self.graphite.add_stats(offsets, prefix='ampel.ztf.kafka.uw.topics')
			self.graphite.send()
		except Exception as e:
			log.error(e)
			return

	def alerts(self, limit=None):
		"""
		Generate alerts until timeout is reached
		:returns: dict instance of the alert content
		:raises StopIteration: when next(fastavro.reader) has dried out
		"""
		topic_stats = defaultdict(lambda: [float('inf'),-float('inf'),0])
		for message in itertools.islice(self._consumer, limit):
			reader = fastavro.reader(io.BytesIO(message.value()))
			alert = next(reader) # raise StopIteration
			stats = topic_stats[message.topic()]
			if alert['candidate']['jd'] < stats[0]:
				stats[0] = alert['candidate']['jd']
			if alert['candidate']['jd'] > stats[1]:
				stats[1] = alert['candidate']['jd']
			stats[2] += 1
			if self.archive_updater:
				self.archive_updater.insert_alert(
					alert, reader.writer_schema, message.partition(), int(1e6*time.time())
				)
			yield alert
		log.info('Got messages from topics: {}'.format(dict(topic_stats)))

	def __iter__(self):
		return self.alerts()
