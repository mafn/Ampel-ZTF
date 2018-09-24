#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ZISetup.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 14.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.AmpelAlert import AmpelAlert
from ampel.core.flags.AlertFlags import AlertFlags
from ampel.core.abstract.AbsT0Setup import AbsT0Setup
from ampel.pipeline.t0.load.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.load.ZIAlertShaper import ZIAlertShaper
from ampel.pipeline.t0.ingest.ZIAlertIngester import ZIAlertIngester

class ZISetup(AbsT0Setup):
	"""
	ZI: Shortcut for ZTFIPAC
	"""

	def __init__(self, serialization="avro", check_reprocessing=True, alert_history_length=30):
		"""
		"""
		
		# Set static AmpelAlert alert flags
		AmpelAlert.set_class_flags(
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

		# Global config whether to check for IPAC PPS reprocessing
		self.check_reprocessing = check_reprocessing

		# Global config defining the std IPAC alert history length.
		# As of June 2018: 30 days
		self.alert_history_length = alert_history_length

		self.serialization = serialization


	def get_alert_supplier(self, alert_loader):
		""" 
		"""
		return AlertSupplier(
			alert_loader, ZIAlertShaper.shape, serialization=self.serialization
		)


	def get_alert_ingester(self, channels, logger):
		"""
		:param channels:
		:param logger: logger instance (python module 'logging')
		"""
		return ZIAlertIngester(
			channels, logger=logger,
			check_reprocessing=self.check_reprocessing,
			alert_history_length=self.alert_history_length
		)
