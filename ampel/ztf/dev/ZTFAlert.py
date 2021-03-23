#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/dev/ZTFAlert.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.06.2018
# Last Modified Date: 31.07.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import fastavro, os, time
from typing import Any, Dict, Optional, List
from ampel.view.LightCurve import LightCurve
from ampel.view.TransientView import TransientView
from ampel.content.DataPoint import DataPoint
from ampel.content.T2Record import T2Record
from ampel.alert.PhotoAlert import PhotoAlert
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.ztf.ingest.ZiT0PhotoPointShaper import ZiT0PhotoPointShaper
from ampel.ztf.ingest.ZiT0UpperLimitShaper import ZiT0UpperLimitShaper


class ZTFAlert:


	@classmethod
	def to_photo_alert(cls, file_path: Optional[str] = None) -> PhotoAlert:
		"""
		Creates and returns an instance of ampel.view.LightCurve using a ZTF IPAC alert.
		"""
		als = ZiAlertSupplier(deserialize="avro")
		if file_path:
			from ampel.alert.load.FileAlertLoader import FileAlertLoader
			als.set_alert_source(FileAlertLoader(files=file_path))

		return next(als)

	@staticmethod
	def _upper_limit_id(el: Dict[str, Any]) -> int:
		return int("%i%s%i" % (
		(2457754.5 - el['jd']) * 1000000,
		str(el['pid'])[8:10],
		round(abs(el['diffmaglim']) * 1000)))

	@classmethod
	def to_lightcurve(cls, file_path: Optional[str] = None, pal: Optional[PhotoAlert] = None) -> LightCurve:
		"""
		Creates and returns an instance of ampel.view.LightCurve using a ZTF IPAC alert.
		This is either created from an already existing ampel.alert.PhotoAlert or
		read through a ampel.ztf.alert.ZiAlertSupplier (default). 
		In the latter case a path to a stored avro file can be given.
		"""

		if pal is None:
			pal = cls.to_photo_alert(file_path)


		# Build upper limit ids (done by ingester for now)
		uls = ZiT0UpperLimitShaper().ampelize(
			[
				{
					**el,
					**{'_id': cls._upper_limit_id(el)}
				}
				for el in (pal.uls if pal.uls else [])
			]
		)
		pps = ZiT0PhotoPointShaper().ampelize([dict(el) for el in pal.pps])
		for collection in uls, pps:
			for pp in collection:
				pp['stock'] = pal.stock_id

		return LightCurve(
			os.urandom(16), # CompoundId
			pal.stock_id,
			tuple(pps), # Photopoints
			tuple(uls), # Upperlimit
			0, # tier
			time.time() # added
		)


	# TODO: incomplete/meaningless/quick'n'dirty method, to improve if need be
	@classmethod
	def to_transientview(cls,
		file_path: Optional[str] = None,
		alert: Optional[PhotoAlert] = None,
		content: Optional[Dict] = None,
		t2_records: Optional[List[T2Record]] = None
	) -> TransientView:
		"""
		Note: incomplete/meaningless//quick'n'dirty method, to improve if need be.
		Creates and returns an instance of ampel.view.LightCurve using a ZTF IPAC alert.
		"""

		if alert is None:
			alert = cls.to_photo_alert(file_path)
		lc = cls.to_lightcurve(pal=alert)

		datapoints: List[DataPoint] = []
		if lc.photopoints:
			datapoints += list(lc.photopoints)
		if lc.upperlimits:
			datapoints += list(lc.upperlimits)

		return TransientView(
			id = alert.stock_id,
			t0 = datapoints,
			t2 = t2_records,
			extra = {
				'names': [alert.name]
			}
		)


	@classmethod
	def _load_alert(cls, file_path: str) -> Optional[Dict]:
		""" """
		with open(file_path, 'rb') as f:
			content = cls._deserialize(f)
		return content


	@staticmethod
	def _deserialize(f) -> Optional[Dict]:
		""" """
		reader = fastavro.reader(f)
		return next(reader, None)
