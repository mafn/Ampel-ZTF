#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/pipeline/t0/load/UWAlertLoader.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import io, time, itertools, logging, uuid, fastavro
from ampel.ztf.pipeline.t0.load.AllConsumingConsumer import AllConsumingConsumer

class UWAlertLoader:
	"""
	Iterable class that loads avro alerts from the Kafka stream 
	provided by University of Washington (UW) 
	"""

	def __init__(self, 
		partnership,
		bootstrap='epyc.astro.washington.edu:9092', 
		group_name=uuid.uuid1(), 
		update_archive=False, 
		timeout=1
	):
		"""
		:param bool partnership: if True, subscribe to ZTF partnership alerts. Otherwise,
	    subscribe only to the public alert stream
		:param str bootstrap: host:port of Kafka server
		:param bytes group_name: consumer group name. Fetchers with the same group name
	    will be load balanced and receive disjoint sets of messages
		:param bool update_archive: if True, fetched alerts will be inserted into 
		the archive db using ampel.pipeline.t0.ArchiveUpdater
		:param int timeout: time to wait for messages before giving up, in seconds
		"""
		topics = ['^ztf_.*_programid1']

		if partnership:
			topics.append('^ztf_.*_programid2')

		if update_archive:
			from ampel.pipeline.config.AmpelConfig import AmpelConfig
			from ampel.ztf.pipeline.t0.ArchiveUpdater import ArchiveUpdater
			self.archive_updater = ArchiveUpdater(
				AmpelConfig.get_config('resources.archive.writer')
			)
		else:
			self.archive_updater = None

		self._consumer = AllConsumingConsumer(
			bootstrap, timeout=timeout, topics=topics, **{'group.id':group_name}
		)

		self.logger = logging.getLogger('UWAlertLoader')


	def alerts(self, limit=None):
		"""
		Generate alerts until timeout is reached
		:returns: dict instance of the alert content
		:raises StopIteration: when next(fastavro.reader) has dried out
		"""
		for message in itertools.islice(self._consumer, limit):
			reader = fastavro.reader(io.BytesIO(message.value()))
			alert = next(reader) # raise StopIteration
			if self.archive_updater:
				self.archive_updater.insert_alert(
					alert, reader.schema, message.partition(), int(1e6*time.time())
				)
			yield alert

		self.logger.info('timed out')

	def __iter__(self):
		return self.alerts()
