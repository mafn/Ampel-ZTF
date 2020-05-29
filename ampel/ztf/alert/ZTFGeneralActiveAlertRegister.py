#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/alert/ZTFGeneralActiveAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2020
# Last Modified Date: 27.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from struct import pack
from typing import Optional, ClassVar, Tuple, Union, BinaryIO, List, Sequence, Set, Dict, Any
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.ztf.alert.ZTFGeneralAlertRegister import ZTFGeneralAlertRegister
from ampel.log.AmpelLogger import AmpelLogger


class ZTFGeneralActiveAlertRegister(ZTFGeneralAlertRegister):
	"""
	Optimized GeneralAlertRegister in that ZTF stock saved with 5 bytes instead of 8 (std Q size).
	That is because:
	In []: 2**36 < to_ampel_id('ZTF33zzzzzzz') < 2**37
	Out[]: True
	Logs: alert_id, filter_res, stock_id
	"""

	__slots__: ClassVar[Tuple[str, ...]] = '_write', 'alert_max', 'alert_min', 'stock_max', 'stock_min' # type: ignore
	_slot_defaults = {'alert_max': 0, 'alert_min': 2**64, 'stock_max': 0, 'stock_min': 2**64, 'ztf_years': set()}

	new_header_size: Union[int, str] = "+4096"
	header_hints: ClassVar[Sequence[str]] = ('alert', 'stock') # type: ignore
	alert_min: int
	alert_max: int
	stock_min: int
	stock_max: int
	ztf_years: Set[int]


	def __init__(self, **kwargs) -> None: # type: ignore[override]

		super().__init__(**kwargs)
		hdr = self.header['payload']
		if 'ztf_years' in hdr:
			self.ztf_years = set(hdr['ztf_years'])


	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:

		alid = alert.id
		if alid > self.alert_max:
			self.alert_max = alid
		if alid < self.alert_min:
			self.alert_min = alid

		sid = alert.stock_id
		if sid > self.stock_max: # type: ignore[operator]
			self.stock_max = sid # type: ignore[assignment]
		if sid < self.stock_min: # type: ignore[operator]
			self.stock_min = sid # type: ignore[assignment]

		if (sid & 15) not in self.ztf_years: # type: ignore[operator]
			self.ztf_years.add(sid & 15) # type: ignore[operator]

		self._write(pack('<QBQ', alert.id, filter_res or 0, alert.stock_id)[:-3])


	def close(self, **kwargs) -> None: # type: ignore[override]

		hdr = self.header['payload']
		if self.ztf_years:
			hdr['ztf_years'] = list(self.ztf_years)

		super().close(**kwargs)



	@classmethod
	def find_stock(cls, # type: ignore[override]
		f: Union[BinaryIO, str], stock_id: Union[int, List[int]], **kwargs
	) -> Optional[List[Tuple[int, ...]]]:
		return super().find_stock(f, stock_id, header_hint_callback=cls._match_ztf_years, **kwargs)


	@staticmethod
	def _match_ztf_years(
		header: Dict[str, Any], stock_id: Union[int, List[int]], logger: Optional[AmpelLogger] = None
	) -> Optional[Union[int, List[int]]]:

		if 'ztf_years' in header:
			zy = header['ztf_years']
		else:
			return stock_id

		if isinstance(stock_id, int):
			if stock_id & 15 in zy:
				if logger: logger.info(f"Header ZTF year check: {stock_id} is eligible") # noqa
				return stock_id

			if logger: logger.info(f"Header ZTF year check: {stock_id} is not eligible") # noqa
			return None

		else:
			ret = [el for el in stock_id if (el & 15) in zy]
			if len(ret) == len(stock_id):
				if logger: logger.info("Header ZTF year check: all stock IDs are eligible") # noqa
				return stock_id

			if logger:
				if len(ret) == 0:
					logger.info("Header ZTF year check: none of the provided stock IDs are eligible")
				else:
					logger.info(f"Header ZTF year check: stock IDs search targets reduced to {ret}")

			return ret
