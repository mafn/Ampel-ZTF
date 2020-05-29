#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/model/ZTFLegacyChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 15.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, ClassVar, List
from ampel.log.AmpelLogger import AmpelLogger
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.model.template.AbsLegacyChannelTemplate import AbsLegacyChannelTemplate


class ZTFLegacyChannelTemplate(AbsLegacyChannelTemplate):

	# static variables (ClassVar type) are ignored by pydantic
	_access: ClassVar = {
		"ztf_uw_private": ["ZTF", "ZTF_PUB", "ZTF_COLLAB"],
		"ztf_uw_public": ["ZTF", "ZTF_PUB"],
		"ztf_uw_caltech": ["ZTF", "ZTF_PUB"]
	}

	# Mandatory implementation
	def get_channel(self, logger: AmpelLogger) -> Dict[str, Any]:
		return {
			**super().get_channel(logger),
			'access': self.__class__._access[self.template]
		}


	# Mandatory implementation
	def get_processes(self, logger: AmpelLogger, first_pass_config: FirstPassConfig) -> List[Dict[str, Any]]:

		# T3 processes
		ret: List[Dict[str, Any]] = [
			self.transfer_channel_parameters(el)
			for el in self.t3_supervize
		]

		ret.insert(0,
			self.craft_t0_process(
				first_pass_config,
				controller = {
					"unit": "ZTFStreamController",
					"config": {
						"priority": "standard",
						"stream": self.template
					}
				},
				stock_ingester = "ZiStockIngester",
				t0_ingester = "ZiAlertContentIngester",
				t1_ingester = ("PhotoCompoundIngester", {"combiner": {"unit": "ZiT1Combiner"}}),
				t2_state_ingester = ("PhotoT2Ingester", {"tags": ["ZTF"]}),
				t2_point_ingester = ("PointT2Ingester", {"tags": ["ZTF"]}),
				t2_stock_ingester = ("StockT2Ingester", {"tags": ["ZTF"]}),
			)
		)

		return ret
