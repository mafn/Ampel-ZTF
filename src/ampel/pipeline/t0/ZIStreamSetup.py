#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ZIDataSource.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 06.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.AmpelAlert import AmpelAlert
from ampel.core.flags.AlertFlags import AlertFlags
from ampel.pipeline.config.AmpelDataSource import AmpelDataSource
from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester

class ZIDataSource(AmpelDataSource):
	"""
	"""

	def __init__(self, 
		serialization="avro", 
		check_reprocessing=True, 
		alert_history_length=30
	):
		"""
		"""

		# Set static AmpelAlert alert flags
		AmpelAlert.add_class_flags(
			AlertFlags.INST_ZTF|AlertFlags.SRC_IPAC
		)

		# Set static AmpelAlert dict keywords
		AmpelAlert.set_alert_keywords(
			{
				"tranId" : "objectId",
				"ppId" : "candid",
				"obsDate" : "jd",
				"filterId" : "fid",
				"mag" : "magpsf"
			}
		)

		self.alert_history_length = alert_history_length 

		if serialization == "json":
			import json
			self.deserialize = lambda f: (json.load(f), {})

		elif serialization == "avro":
			import fastavro
			def deserialize(f):
				reader = fastavro.reader(f)
				return next(reader, None), reader.schema
			self.deserialize = deserialize

		else:
			raise NotImplementedError(
				"Deserialization format %s not implemented" % serialization
			)

		# Global config whether to check for IPAC PPS reprocessing
		self.check_reprocessing = check_reprocessing

		# Global config defining the std IPAC alert history length. 
		# As of June 2018: 30 days
		self.alert_history_length = alert_history_length


	def get_shape_func(self):
		""" """
		return ZIAlertShaper().shape


	def get_deserialize_func(self):
		""" """
		return self.deserialize


	def get_ingester(self, channels, logger):
		""" 
		"""
		return ZIAlertIngester(
			channels, logger=logger, 
			check_reprocessing = self.check_reprocessing,
			alert_history_length = self.alert_history_length
		)
