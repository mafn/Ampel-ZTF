#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/ingest/ZiStockIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 20.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Any, Union
from ampel.type import StockId
from ampel.ztf.util.ZTFIdMapper import to_ztf_id
from ampel.ingest.StockIngester import StockIngester


class ZiStockIngester(StockIngester):

	# Override
	tag: List[Union[int, str]] = ["ZTF"]

	# Override
	def get_setOnInsert(self, stock_id: StockId) -> Dict[str, Any]:
		return {
			'tag': self.tag,
			'name': [to_ztf_id(stock_id)] # type: ignore[arg-type]
		}
