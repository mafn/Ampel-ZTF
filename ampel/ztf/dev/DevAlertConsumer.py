#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/dev/DevAlertConsumer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 30.07.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import logging, time, sys, fastavro, tarfile # type: ignore[import]
from ampel.alert.PhotoAlert import PhotoAlert


class DevAlertConsumer:
	"""
	For each alert: load, filter, ingest.
	"""

	def __init__(self, alert_filter, save="alert", include_cutouts=True):
		"""
		Parameters
		-----------

		alert_filter:
			Instance of a t0 alert filter. It must implement method:
			process(<instance of ampel.alert.PhotoAlert>)

		save:
			either
				* 'alert': references to PhotoAlert instances will be kept
				* 'objectId': only objectId strings will be kept
				* 'candid': only candid integers will be kept
				* 'objectId_candid': tuple ('candid', 'objectId') will be kept

		include_cutouts:
			If True, PhotoAlert will contain cutouts images as attribute 'cutouts'
		"""
		logging.basicConfig( # Setup logger
			format = '%(asctime)s %(levelname)s %(message)s',
			datefmt = "%Y-%m-%d %H:%M:%S",
			level = logging.INFO,
			stream = sys.stdout
		)

		self._logger = logging.getLogger()
		self._alert_filter = alert_filter
		self._accepted_alerts = []
		self._rejected_alerts = []
		self.save = save
		self.include_cutouts = include_cutouts


	def get_accepted_alerts(self):
		return self._accepted_alerts


	def get_rejected_alerts(self):
		return self._rejected_alerts


	def process_tar(self, tar_file_path, tar_mode="r:gz", iter_max=5000, iter_offset=0):
		""" For each alert: load, filter, ingest """
		self.tar_file = tarfile.open(tar_file_path, mode=tar_mode)
		return self._run(self.tar_file, self._unpack, iter_max=iter_max, iter_offset=iter_offset	)


	def process_loaded_alerts(self, list_of_alerts, iter_max=5000):
		""" For each alert: load, filter, ingest """
		return self._run(list_of_alerts, lambda x: x, iter_max=iter_max)


	def _run(self, iterable, load, iter_max=5000, iter_offset=0):
		""" For each alert: load, filter, ingest  """

		self._accepted_alerts = []
		self._rejected_alerts = []

		run_start = time.time()
		iter_count = 0

		# Iterate over alerts
		for content in iterable:
			if iter_count < iter_offset:
				iter_count += 1
				continue

			alert = load(content)
			if alert is None:
				break

			# filter alert
			self._filter(alert)

			iter_count += 1
			if iter_count == (iter_max + iter_offset):
				self._logger.info("Reached max number of iterations")
				break

		self._logger.info(
			"%i alert(s) processed (time required: %is)" %
			(iter_count - iter_offset, int(time.time() - run_start))
		)

		# Return number of processed alerts
		return iter_count - iter_offset


	def _unpack(self, tar_info):

		# Reach end of archive
		if tar_info is None:
			self._logger.info("Reached end of tar files")
			self.tar_file.close()
			return

		if not tar_info.isfile():
			return

		# deserialize extracted alert content
		alert_content = self._deserialize(
			self.tar_file.extractfile(tar_info)
		)

		# Create PhotoAlert instance
		alert = PhotoAlert(
			alert_content['objectId'],
			alert_content['objectId'],
			*self._shape(alert_content)
		)

		if self.include_cutouts:
			alert.data['cutouts'] = {
				k: alert_content.get(k).get('stampData')
				for k in ('cutoutScience', 'cutoutTemplate', 'cutoutDifference')
				if alert_content.get(k)
			}

		return alert


	def _filter(self, alert):

		filter_result = self._alert_filter.process(alert)
		if filter_result is None or filter_result < 0:
			self._logger.debug(
				"- Rejecting %i (objectId: %s)" %
				(alert.pps[0]['candid'], alert.stock_id)
			)
			target_array = self._rejected_alerts
		else:
			self._logger.debug(
				"+ Ingesting %i (objectId: %s)" %
				(alert.pps[0]['candid'], alert.stock_id)
			)
			target_array = self._accepted_alerts

		if self.save == "alert":
			target_array.append(alert)
		elif self.save == 'objectId':
			target_array.append(alert.id)
		elif self.save == 'candid':
			target_array.append(alert.pps[0]['candid'])
		elif self.save == 'objectId_candid':
			target_array.append((alert.id, alert.pps[0]['candid']))


	def _deserialize(self, f):
		reader = fastavro.reader(f)
		return next(reader, None)


	def _shape(self, alert_content):
		"""
		Returns datapoints, photopoints, upperlimits
		"""

		if alert_content.get('prv_candidates') is not None:
			dps = [el for el in alert_content['prv_candidates']]
			pps = [el for el in alert_content['prv_candidates'] if el.get('candid') is not None]
			dps.insert(0, alert_content['candidate'])
			pps.insert(0, alert_content['candidate'])
			return dps, pps, [el for el in alert_content['prv_candidates'] if el.get('candid') is None]
		else:
			return [alert_content['candidate']], [alert_content['candidate']], None
