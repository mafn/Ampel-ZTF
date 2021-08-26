#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/ingest/ZiMongoMuxer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 25.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Any, Optional, Tuple
from pymongo import UpdateOne
from ampel.types import StockId
from ampel.content.DataPoint import DataPoint
from ampel.util.mappings import unflatten_dict
from ampel.mongo.utils import maybe_use_each
from ampel.abstract.AbsT0Muxer import AbsT0Muxer

class ConcurrentUpdateError(Exception):
	"""
	Raised when the t0 collection was updated during ingestion
	"""
	...

class ZiMongoMuxer(AbsT0Muxer):
	"""
	This class compares info between alert and DB so that only the needed info is ingested later.
	Also, it marks potentially reprocessed datapoints as superseded.

	:param check_reprocessing: whether the ingester should check if photopoints were reprocessed
	(costs an additional DB request per transient). Default is (and should be) True.

	:param alert_history_length: alerts must not contain all available info for a given transient.
	IPAC generated alerts for ZTF for example currently provide a photometric history of 30 days.
	Although this number is unlikely to change, there is no reason to use a constant in code.
	"""

	check_reprocessing: bool = True
	alert_history_length: int = 30

	# Be idempotent for the sake it (not required for prod)
	idempotent: bool = False

	# True: Alert + DB dps will be combined into state
	# False: Only the alert dps will be combined into state
	db_complete: bool = True

	# Standard projection used when checking DB for existing PPS/ULS
	projection = {
		'_id': 0, 'id': 1, 'tag': 1, 'channel': 1, 'excl': 1, 'stock': 1, 'body.jd': 1,
		'body.programid': 1, 'body.fid': 1, 'body.rcid': 1, 'body.magpsf': 1
	}


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)

		# used to check potentially already inserted pps
		self._photo_col = self.context.db.get_collection("t0")
		self._projection_spec = unflatten_dict(self.projection)


	def process(self,
		dps: List[DataPoint],
		stock_id: Optional[StockId] = None
	) -> Tuple[Optional[List[DataPoint]], Optional[List[DataPoint]]]:
		"""
		:param dps: datapoints from alert
		:param stock_id: stock id from alert
		Attempt to determine which pps/uls should be inserted into the t0 collection,
		and which one should be marked as superseded.
		"""
		# IPAC occasionally issues multiple subtraction candidates for the same
		# exposure and source, and these may be received in parallel by two
		# AlertConsumers.
		for _ in range(10):
			try:
				return self._process(dps, stock_id)
			except ConcurrentUpdateError:
				continue
		else:
			raise ConcurrentUpdateError(f"More than 10 iterations ingesting alert {dps[0]['id']}")


	def _process(self,
		dps: List[DataPoint],
		stock_id: Optional[StockId] = None
	) -> Tuple[Optional[List[DataPoint]], Optional[List[DataPoint]]]:
		"""
		:param dps: datapoints from alert
		:param stock_id: stock id from alert
		Attempt to determine which pps/uls should be inserted into the t0 collection,
		and which one should be marked as superseded.
		"""

		# Part 1: gather info from DB and alert
		#######################################

		# New pps/uls lists for db loaded datapoints
		dps_db: List[DataPoint] = list(
			self._photo_col.find({'stock': stock_id}, self.projection)
		)

		ops = []
		if self.check_reprocessing:
			add_update = lambda op: ops.append(op)
		else:
			add_update = self.updates_buffer.add_t0_update

		# Create set with datapoint ids from alert
		ids_dps_alert = {el['id'] for el in dps}

		# python set of ids of datapoints from DB
		ids_dps_db = {el['id'] for el in dps_db}

		# uniquify photopoints by jd, rcid. For duplicate points,
		# choose the one with the larger id
		ids_dps_superseded = {}
		unique_dps: Dict[Tuple[float, int], DataPoint] = {}

		for dp in sorted(dps + dps_db, key = lambda x: x['id']):

			# jd alone is actually enough for matching pps reproc, but an upper limit can
			# be associated with multiple stocks at the same jd. here, match also by rcid
			key = (dp['body']['jd'], dp['body']['rcid'])

			if key in unique_dps:
				if dp['id'] > unique_dps[key]['id']:
					ids_dps_superseded[unique_dps[key]['id']] = dp['id']
					unique_dps[key] = dp
				elif dp['id'] < unique_dps[key]['id']:
					ids_dps_superseded[dp['id']] = unique_dps[key]['id']
				else:
					# DB datapoints should be prefered as they contain channel association info
					if dp in dps_db:
						unique_dps[key] = dp
			else:
				unique_dps[key] = dp
		print(ids_dps_superseded)
				

		# Part 2: Insert new data points
		################################

		# Difference between candids from the alert and candids present in DB
		ids_dps_to_insert = ids_dps_alert - ids_dps_db

		for dp in dps:

			if dp['id'] in ids_dps_to_insert:

				add_to_set = {'stock': stock_id}

				# If alerts were received out of order, this point may already be superseded.
				if (
					self.check_reprocessing and
					dp['id'] in ids_dps_superseded and
					'SUPERSEDED' not in dp['tag']
				):

					# NB: here we modify the point in place, so the SUPERSEDED
					# tag remains in place if there is a second pass
					dp['tag'] = list(dp['tag'])
					dp['tag'].append('SUPERSEDED') # type: ignore[attr-defined]
					add_to_set['newId'] = ids_dps_superseded[dp['id']]

					self.logger.info(
						f'Marking datapoint {dp["id"]} '
						f'as superseded by {ids_dps_superseded[dp["id"]]}'
					)

				add_to_set['tag'] = maybe_use_each(dp['tag'])

				# Unconditionally update the doc
				add_update(
					UpdateOne(
						{'id': dp['id']},
						{
							'$setOnInsert': {
								k: v for k, v in dp.items()
								if k not in {'tag', 'stock'}
							},
							'$addToSet': add_to_set
						},
						upsert=True
					)
				)


		# Part 3: Update data points that were superseded
		#################################################

		if self.check_reprocessing:
			for dp in dps_db or []:
				if dp['id'] in ids_dps_superseded and 'SUPERSEDED' not in dp['tag']:

					self.logger.info(
						f'Marking datapoint {dp["id"]} '
						f'as superseded by {ids_dps_superseded[dp["id"]]}'
					)

					dp['tag'] = list(dp['tag'])
					dp['tag'].append('SUPERSEDED') # type: ignore[attr-defined]

					add_update(
						UpdateOne(
							{'id': dp['id']},
							{
								'$addToSet': {
									'newId': ids_dps_superseded[dp['id']],
									'tag': 'SUPERSEDED'
								}
							}
						)
					)

		# Part 4: commit ops and check for conflicts
		############################################
		if self.check_reprocessing:
			# Commit ops, retrying on upsert races
			if ops:
				self.updates_buffer.call_bulk_write('t0', ops)
			# If another query returns docs not present in the first query, the
			# set of superseded photopoints may be incomplete.
			if concurrent_updates := (
				{
					doc['id']
					for doc in self._photo_col.find({'stock': stock_id}, {'id': 1})
				} \
				- (ids_dps_db | ids_dps_alert)
			):
				raise ConcurrentUpdateError(f"t0 collection contains {len(concurrent_updates)} extra photopoints: {concurrent_updates}")

		# The union of the datapoints drawn from the db and
		# from the alert will be part of the t1 document
		if self.db_complete:

			# DB might contain datapoints newer than the newest alert dp
			# https://github.com/AmpelProject/Ampel-ZTF/issues/6
			latest_alert_jd = dps[0]['body']['jd']
			dps_combine = [
				dp for dp in unique_dps.values()
				if dp['body']['jd'] <= latest_alert_jd
			]

			# Project datapoint the same way whether they were drawn from the db or from the alert.
			if self.idempotent and self.projection:
				for i, el in enumerate(dps_combine):
					if el in ids_dps_to_insert:
						dps_combine[i] = self._project(el, self._projection_spec)
		else:
			dps_combine = dps

		return [dp for dp in dps if dp['id'] in ids_dps_to_insert], dps_combine


	def _project(self, doc, projection):

		out: Dict[str, Any] = {}
		for key, spec in projection.items():

			if key not in doc:
				continue

			if isinstance(spec, dict):
				item = doc[key]
				if isinstance(item, list):
					out[key] = [self._project(v, spec) for v in item]
				elif isinstance(item, dict):
					out[key] = self._project(item, spec)
			else:
				out[key] = doc[key]

		return out
