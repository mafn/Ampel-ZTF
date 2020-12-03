#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/ingest/ZiT0PhotoPointShaper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Any, Iterable, Optional
from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.content.DataPoint import DataPoint
from ampel.log.AmpelLogger import AmpelLogger
from ampel.ztf.ingest.tags import tags


class ZiT0PhotoPointShaper(AbsT0Unit):
	"""
	This class 'shapes' datapoints in a format suitable
	to be saved into the ampel database
	"""

	# override
	logger: Optional[AmpelLogger] # type: ignore[assignment]

	# Mandatory implementation
	def ampelize(self, arg: Iterable[Dict[str, Any]]) -> List[DataPoint]:
		"""
		:param arg: sequence of unshaped pps
		IMPORTANT:
		1) This method *modifies* the input dicts (it removes 'candid' and programpi),
		even if the unshaped pps are ReadOnlyDict instances
		2) 'stock' (prev. called tranId) is not set here on purpose
		since it would then conflicts with the $addToSet operation
		"""

		ret_list: List[DataPoint] = []
		setitem = dict.__setitem__
		popitem = dict.pop

		for photo_dict in arg:

			# Cut path if present
			if photo_dict.get('pdiffimfilename'):
				setitem(
					photo_dict, 'pdiffimfilename',
					photo_dict['pdiffimfilename'] \
						.split('/')[-1] \
						.replace('.fz', '')
				)

			ret_list.append(
				{
					'_id': photo_dict['candid'],
					'tag': tags[photo_dict['programid']][photo_dict['fid']],
					'body': photo_dict
				}
			)

			popitem(photo_dict, 'candid', None)
			popitem(photo_dict, 'programpi', None)


		return ret_list
