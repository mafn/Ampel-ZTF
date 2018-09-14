#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/ZIAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 14.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.t0.ZIAlertShaper import ZIAlertShaper
import fastavro

class ZIAlertSupplier:
	"""
	ZTF IPAC UW (University of Washington) alert supplier.
	Intrument: ZTF
	Image processing: IPAC
	Alert stream: Kafka stream made of alerts serialized in avro format (UW)
	"""
 
	def __init__(self, alert_loader):
		"""
		:param alert_loader: loads and returns alerts file like objects. Class must be iterable.
		"""
		self.alert_loader = alert_loader
 

	def __iter__(self):
		return self

	
	def __next__(self):
		"""
		:returns: a dict with a format that the AMPEL AlertProcessor understands or 
		None if the alert_loader has dried out.
		"""
		fileobj, partition_id = next(self.alert_loader)
		reader = fastavro.reader(fileobj)
		return ZIAlertShaper.shape(next(reader, None))
