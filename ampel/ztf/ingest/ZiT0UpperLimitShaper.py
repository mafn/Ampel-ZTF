#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/ingest/ZiT0UpperLimitShaper.py
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


class ZiT0UpperLimitShaper(AbsT0Unit):
	"""
	This class 'shapes' upper limits in a format suitable
	to be saved into the ampel database
	"""

	# override
	logger: Optional[AmpelLogger] # type: ignore[assignment]


	# Mandatory implementation
	def ampelize(self, arg: Iterable[Dict[str, Any]]) -> List[DataPoint]:
		"""
		'stock' (prev. called tranId) is not set here on purpose
		since it would then conflicts with the $addToSet operation
		"""

		return [
			{
				'_id': photo_dict['_id'],
				'tag': tags[photo_dict['programid']][photo_dict['fid']],
				'body': {
					'jd': photo_dict['jd'],
					'diffmaglim': photo_dict['diffmaglim'],
					'rcid': photo_dict['rcid'],
					'fid': photo_dict['fid']
					#'pdiffimfilename': fname
					#'pid': photo_dict['pid'],
					# programid is contained in alTags
					#'programid': photo_dict['programid'],
				}
			}
			for photo_dict in arg
		]
