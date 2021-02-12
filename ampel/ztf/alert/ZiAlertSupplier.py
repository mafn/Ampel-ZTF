#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/alert/ZiAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 28.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal, List, Union, Callable, Any, Dict
from ampel.ztf.util.ZTFIdMapper import to_ampel_id
from ampel.alert.PhotoAlert import PhotoAlert
from ampel.view.ReadOnlyDict import ReadOnlyDict
from ampel.abstract.AbsAlertSupplier import AbsAlertSupplier


class ZiAlertSupplier(AbsAlertSupplier[PhotoAlert]):
	"""
	Iterable class that, for each alert payload provided by the underlying alert_loader,
	returns an PhotoAlert instance.
	"""

	# Override default
	deserialize: Union[None, Literal["avro", "json"], Callable[[Any], Dict]] = "avro"
	stat_pps: int = 0
	stat_uls: int = 0


	def __next__(self) -> PhotoAlert:
		"""
		:returns: a dict with a structure that AlertProcessor understands
		:raises StopIteration: when alert_loader dries out.
		:raises AttributeError: if alert_loader was not set properly before this method is called
		"""
		d = self.deserialize(
			next(self.alert_loader) # type: ignore
		)

		if d['prv_candidates']:

			pp = ReadOnlyDict(d['candidate'])
			dps: List[ReadOnlyDict] = [pp]
			uls: List[ReadOnlyDict] = []
			pps: List[ReadOnlyDict] = [pp]

			for el in d['prv_candidates']:

				# Upperlimit
				if el.get('candid') is None:

					# rarely, meaningless upper limits with negativ
					# diffmaglim are provided by IPAC
					if el['diffmaglim'] < 0:
						continue

					ul = ReadOnlyDict(
						jd = el['jd'],
						fid = el['fid'],
						pid = el['pid'],
						diffmaglim = el['diffmaglim'],
						programid = el['programid'],
						pdiffimfilename = el.get('pdiffimfilename')
					)

					dps.append(ul)
					uls.append(ul)

				# PhotoPoint
				else:
					pp = ReadOnlyDict(el)
					dps.append(pp)
					pps.append(pp)

			# Update stats
			self.stat_pps += len(pps)
			if uls:
				self.stat_uls += len(uls)

			return PhotoAlert(
				id = d['candid'], # alert id
				stock_id = to_ampel_id(d['objectId']), # internal ampel id
				dps = tuple(dps),
				pps = tuple(pps),
				uls = tuple(uls) if uls else None,
				name = d['objectId'], # ZTF name
			)

		self.stat_pps += 1
		datapoints = (ReadOnlyDict(d['candidate']),)

		# No "previous candidate"
		return PhotoAlert(
			id = d['candid'], # alert id
			stock_id = to_ampel_id(d['objectId']), # internal ampel id
			dps = datapoints,
			pps = datapoints,
			uls = None,
			name = d['objectId'], # ZTF name
		)


	def get_stats(self, reset: bool = True) -> Dict[str, Any]:

		ret = {'pps': self.stat_pps, 'uls': self.stat_uls}
		if reset:
			self.stat_pps = 0
			self.stat_uls = 0
		return ret
