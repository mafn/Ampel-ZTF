#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ZISetup.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 18.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.AmpelAlert import AmpelAlert
from ampel.core.flags.AlertFlags import AlertFlags
from ampel.core.flags.LogRecordFlags import LogRecordFlags
from ampel.core.abstract.AbsT0Setup import AbsT0Setup
from ampel.pipeline.t0.load.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.load.ZIAlertShaper import ZIAlertShaper
from ampel.pipeline.t0.ingest.ZIAlertIngester import ZIAlertIngester

class ZISetup(AbsT0Setup):
	"""
	ZI: Shortcut for ZTFIPAC
	Class that sets up various static fields in AmpelAlert and 
	provides an alert supplier and an alert ingester that 
	work with the stream of alerts provided by ZTF/IPAC
	"""

	survey_id = "ZTFIPAC"

	def __init__(self, serialization="avro", check_reprocessing=True, alert_history_length=30):
		"""
		:param str serialization: for now: 'avro' and 'json' are supported
		:param bool check_reprocessing: whether the ingester should check if photopoints were reprocessed
		(costs an additional DB request per transient). Default is (and should be) True.
		:param int alert_history_length: IPAC currently provides us with a photometric history of 30 days.
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


	def get_log_flags(self):
		""" """
		# pylint: disable=no-member
		return LogRecordFlags.INST_ZTF|LogRecordFlags.SRC_IPAC


	def get_alert_supplier(self, alert_loader):
		""" 
		:param alert_loader: iterable instance that returns the content of alerts
		:returns: instance of ampel.pipeline.t0.load.AlertSupplier
		"""
		return AlertSupplier(
			alert_loader, ZIAlertShaper.shape, serialization=self.serialization
		)


	def get_alert_ingester(self, channels, logger):
		"""
		:param channels: list of ampel.pipeline.config.Channel instances
		:param logger: logger instance (python module 'logging')
		"""
		return ZIAlertIngester(
			channels, logger=logger,
			check_reprocessing=self.check_reprocessing,
			alert_history_length=self.alert_history_length
		)
