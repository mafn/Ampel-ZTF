#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File				: Ampel-ZTF/ampel/model/ZTFLegacyChannelTemplate.py
# License			: BSD-3-Clause
# Author			: vb <vbrinnel@physik.hu-berlin.de>
# Date				: 16.10.2019
# Last Modified Date: 11.08.2020
# Last Modified By	: Jakob van Santen <jakob.van.santen@desy.de>

from typing import Dict, Any, ClassVar, List, Union, Optional, Sequence, Literal
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
		ret: List[Dict[str, Any]] = []

		for index, el in enumerate(self.t3_supervise):
			# populate name and tier if unset
			name = el.get("name", f"summary_{index:02d}")
			process_name = f"{self.channel}|T3|{name}"
			ret.append(self.transfer_channel_parameters({
				**el,
				**{"name": process_name, "tier": 3}
			}))

		ret.insert(0,
			self.craft_t0_process(
				first_pass_config,
				controller = {
					"unit": "ZTFAlertStreamController",
					"config": {
						"priority": "standard",
						"source": {
							"stream": self.template,
							**first_pass_config['resource']['ampel-ztf/kafka']
						}
					}
				},
				stock_ingester = "ZiStockIngester",
				t0_ingester = "ZiAlertContentIngester",
				t1_ingester = ("PhotoCompoundIngester", {"combiner": {"unit": "ZiT1Combiner"}}),
				t2_state_ingester = ("PhotoT2Ingester", {"tags": ["ZTF"]}),
				t2_point_ingester = ("DualPointT2Ingester", {"tags": ["ZTF"]}),
				t2_stock_ingester = ("StockT2Ingester", {"tags": ["ZTF"]}),
			)
		)

		return ret
