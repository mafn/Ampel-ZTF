#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/ingest/ZiAlertContentIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 21.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo import UpdateOne
from typing import Dict, List, Any
from ampel.ztf.ingest.ZiT0PhotoPointShaper import ZiT0PhotoPointShaper
from ampel.ztf.ingest.ZiT0UpperLimitShaper import ZiT0UpperLimitShaper
from ampel.content.DataPoint import DataPoint
from ampel.alert.PhotoAlert import PhotoAlert
from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.abstract.ingest.AbsAlertContentIngester import AbsAlertContentIngester


class ZiAlertContentIngester(AbsAlertContentIngester[PhotoAlert, DataPoint]):
	"""
	This class 'ingests' alerts (if they have passed the alert filter):
	it compares info between alert and DB and inserts only the needed info.
	Before new photopoints or upper limits are inserted into the database,
	they are customized (or 'ampelized' if you will),
	in order to later enable the use of short and flexible queries.
	The cutomizations are light, most of the original information is kept.
	For example, in the case of ZiPhotoDataShaper:
		* The field candid is renamed in _id
		* A new field 'tag' is created
		...

	:param check_reprocessing: whether the ingester should check if photopoints were reprocessed
	(costs an additional DB request per transient). Default is (and should be) True.

	:param alert_history_length: see super class docstring
	"""

	check_reprocessing: bool = True

	# Set default for alert_history_length
	alert_history_length: int = 30

	# Associated T0 units
	pp_shaper: AbsT0Unit = ZiT0PhotoPointShaper()
	ul_shaper: AbsT0Unit = ZiT0UpperLimitShaper()

	# JD2017 is used to defined upper limits primary IDs
	JD2017: float = 2457754.5

	# Standard projection used when checking DB for existing PPS/ULS
	projection: Dict[str, int] = {
		'_id': 1, 'tag': 1, 'excl': 1,
		'body.jd': 1, 'body.fid': 1, 'body.rcid': 1, 'body.magpsf': 1
	}

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)

		# used to check potentially already inserted pps
		self._photo_col = self.context.db.get_collection("t0")

		self.stat_pps_reprocs = 0
		self.stat_pps_inserts = 0
		self.stat_uls_inserts = 0


	def ingest(self, alert: PhotoAlert) -> List[DataPoint]:
		"""
		This method is called by the AlertProcessor for alerts passing at least one T0 channel filter.
		Photopoints, transients and  t2 documents are created and saved into the DB.
		Note: Some dict instances referenced in pps_alert and uls_alert might be modified by this method.
		"""

		# Part 1: gather info from DB and alert
		#######################################

		# New pps/uls lists for db loaded datapoints
		pps_db: List[DataPoint] = []
		uls_db: List[DataPoint] = []

		add_update = self.updates_buffer.add_t0_update

		# Load existing photopoint and upper limits from DB (if any)
		# Note: a 'stock' ID should be specific to one instrument
		for el in self._photo_col.find({'stock': alert.stock_id}, self.projection):
			if el['_id'] > 0:
				pps_db.append(el) # Photopoint
			else:
				uls_db.append(el) # Upper limit

		# New lists of datapoints to insert (filling occurs later)
		pps_to_insert: List[DataPoint] = []
		uls_to_insert: List[DataPoint] = []

		# Create set with pp ids from alert
		ids_pps_alert = {el['candid'] for el in alert.pps}

		# python set of ids of photopoints from DB
		ids_pps_db = {el['_id'] for el in pps_db}

		# Set of uls ids from alert
		ids_uls_alert = set()

		# Create unique ids for the upper limits from alert
		# Concrete example:
		# {
		#   'diffmaglim': 19.024799346923828,
		#   'fid': 2,
		#   'jd': 2458089.7405324,
		#   'pdiffimfilename': '/ztf/archive/sci/2017/1202/240532/ \
		#                      ztf_20171202240532_000566_zr_c08_o_q1_scimrefdiffimg.fits.fz',
		#   'pid': 335240532815,
		#   'programid': 0
		# }
		# -> generated ID: -3352405322819025

		# Loop through upper limits from alert
		if alert.uls:
			setitem = dict.__setitem__
			for uld in alert.uls:

				# extract quadrant number from pid (not avail as dedicate key/val)
				rcid = str(uld['pid'])[8:10]
				setitem(uld, 'rcid', int(rcid))

				# Update dict created by fastavro
				setitem(uld, '_id', int(
					'%i%s%i' % (
						# Convert jd float into int by multiplying it by 10**6, we thereby
						# drop the last digit (milisecond) which is pointless for our purpose
						(self.JD2017 - uld['jd']) * 1000000,
						# cut of mag float after 3 digits after coma
						rcid, round(uld['diffmaglim'] * 1000)
					)
				))

			ids_uls_alert.add(uld['_id'])

		# python set of ids of upper limits from DB
		ids_uls_db = {el['_id'] for el in uls_db}



		# Part 2: Determine which datapoint is new
		##########################################

		# Difference between candids from the alert and candids present in DB
		ids_pps_to_insert = ids_pps_alert - ids_pps_db
		ids_uls_to_insert = ids_uls_alert - ids_uls_db

		# If the photopoints already exist in DB

		# PHOTO POINTS
		if ids_pps_to_insert:

			# ForEach photopoint not existing in DB: rename candid into _id, add tags
			# Attention: ampelize *modifies* dict instances loaded by fastavro
			pps_to_insert = self.pp_shaper.ampelize(
				(pp for pp in alert.pps if pp['candid'] in ids_pps_to_insert)
			)
			self.stat_pps_inserts += len(pps_to_insert)

			for pp in pps_to_insert:
				add_update(
					UpdateOne(
						{'_id': pp['_id']},
						{
							'$setOnInsert': pp,
							'$addToSet': {'stock': alert.stock_id}
						},
						upsert=True
					)
				)

		# UPPER LIMITS
		if ids_uls_to_insert:

			# For each upper limit not existing in DB: remove dict key with None values
			# Attention: ampelize *modifies* dict instances loaded by fastavro
			uls_to_insert = self.ul_shaper.ampelize(
				(ul for ul in alert.uls if ul['_id'] in ids_uls_to_insert) # type: ignore[union-attr]
			)
			self.stat_uls_inserts += len(uls_to_insert)

			# Insert new upper limit into DB
			for ul in uls_to_insert:
				add_update(
					UpdateOne(
						{'_id': ul['_id']},
						{
							'$setOnInsert': ul,
							'$addToSet': {'stock': alert.stock_id}
						},
						upsert=True
					)
				)



		# Part 3: check for reprocessed photopoints
		###########################################

		# NOTE: this procedure will *update* selected the dict instances
		# loaded from DB (from the lists: pps_db and uls_db)

		# Difference between candids from db and candids from alert
		ids_in_db_not_in_alert = (ids_pps_db | ids_uls_db) - (ids_pps_alert | ids_uls_alert)

		# If the set is not empty, either some transient info is older that alert_history_length days
		# or some photopoints were reprocessed
		if self.check_reprocessing and ids_in_db_not_in_alert:

			# Ignore ppts in db older than alert_history_length days
			min_jd = alert.pps[0]['jd'] - self.alert_history_length
			ids_in_db_older_than_xx_days = {el['_id'] for el in pps_db + uls_db if el['body']['jd'] < min_jd}
			ids_superseded = ids_in_db_not_in_alert - ids_in_db_older_than_xx_days

			# pps/uls reprocessing occured at IPAC
			if ids_superseded:

				# loop through superseded photopoint
				for photod_db_superseded in filter(
					lambda x: x['_id'] in ids_superseded, pps_db + uls_db
				):

					# Match these with new alert data (already 'shaped' by the ampelize method)
					for new_meas in filter(lambda x:
						# jd alone is actually enough for matching pps reproc
						x['body']['jd'] == photod_db_superseded['body']['jd'] and
						x['body']['rcid'] == photod_db_superseded['body']['rcid'],
						pps_to_insert + uls_to_insert
					):

						self.logd['logs'].append(
							f'Marking photodata {photod_db_superseded["_id"]} '
							f'as superseded by {new_meas["_id"]}'
						)

						# Update tags in dict loaded by fastavro
						# (required for t2 & compounds doc creation)
						if 'SUPERSEDED' not in photod_db_superseded['tag']:
							# Mypy ignore note: tag is declared as Sequence but we have a List for sure
							photod_db_superseded['tag'].append('SUPERSEDED') # type: ignore

						# Create and append pymongo update operation
						self.stat_pps_reprocs += 1
						add_update(
							UpdateOne(
								{'_id': photod_db_superseded['_id']},
								{
									'$addToSet': {
										'newId': new_meas['_id'],
										'tag': ['SUPERSEDED']
									}
								}
							)
						)
			else:
				self.logd['logs'].append('Transient data older than 30 days exist in DB')


		# If no photopoint exists in the DB, then this is a new transient
		if not ids_pps_db:
			self.logd['extra']['new'] = True

		# Photo data that will be part of the compound
		datapoints = [
			el for el in pps_db + pps_to_insert + uls_db + uls_to_insert
			# https://github.com/AmpelProject/Ampel-ZTF/issues/6
			if el['body']['jd'] <= alert.pps[0]['jd']
		]

		return sorted(datapoints, key=lambda k: k['body']['jd'], reverse=True)


	# Mandatory implementation
	def get_stats(self, reset: bool = True) -> Dict[str, Any]:

		ret = {
			'pps': self.stat_pps_inserts,
			'uls': self.stat_uls_inserts,
			'reproc': self.stat_pps_reprocs
		}

		if reset:
			self.stat_pps_reprocs = 0
			self.stat_pps_inserts = 0
			self.stat_uls_inserts = 0

		return ret
