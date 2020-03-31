#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/model/ZTFLegacyChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 24.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger
from typing import Dict, Any, ClassVar, List
from ampel.model.template.LegacyChannelTemplate import LegacyChannelTemplate


class ZTFLegacyChannelTemplate(LegacyChannelTemplate):

	# ClassVar type tells pydantic to ignore '_access'
	_access: ClassVar = {
		"ztf_uw_private": ["ZTF", "ZTF_PUB", "ZTF_COLLAB"],
		"ztf_uw_public": ["ZTF", "ZTF_PUB"],
		"ztf_uw_caltech": ["ZTF", "ZTF_PUB"]
	}

	def get_channel(self, logger: Logger) -> Dict[str, Any]:
		return {
			"channel": self.channel,
			"distrib": self.distrib,
			"source": self.source,
			"active": self.active,
			"access": ZTFLegacyChannelTemplate._access[self.template],
			"contact": self.contact,
			"policy": self.policy
		}


	def get_processes(self, units_config: Dict[str, Any], logger: Logger) -> List[Dict[str, Any]]:

		t3_procs = self.t3_supervize if self.t3_supervize else []

		for t3_proc in t3_procs:
			t3_proc['distrib'] = self.distrib
			t3_proc['source'] = self.source

		ret: List[Dict[str, Any]] = [
			{
				"tier": 0,
				"schedule": "super",
				"active": self.active,
				"distrib": self.distrib,
				"source": self.source,
				"name": f"{self.channel}/T0/{self.template}",
				"controller": {
					"unit": "ZTFStreamController",
					"config": {
						"priority": "standard",
						"stream": self.template
					}
				},
				"processor": {
					"unit": "ZTFAlertProcessor",
					"config": {
						"publish_stats": ["graphite", "processDoc"],
						"directives": {
							"channel": self.channel,
							"auto_complete": self.auto_complete,
							"filter": self.t0_filter.dict(
								skip_defaults=True, by_alias=True
							),
							"t0_add": {},
							"stock_update": {
								"unit": "ZiStockIngester"
							}
						}
					}
				}
			}
		] + t3_procs

		directives = ret[0]['processor']['config']['directives']

		if t2_state_units := self.get_t2_units(units_config, "AbsLightCurveT2Unit"):
			directives['t0_add']['t1_combine'] = [
				{
					"unit": "PhotoCompoundIngester",
					"config": {"combiner": {"unit": "ZiT1Combiner"}},
					"t2_compute": {
						"ingester": "PhotoT2Ingester",
						"config": {"tags": ["ZTF"]},
						"units": t2_state_units
					}
				}
			]

		if t2_stock_units := self.get_t2_units(units_config, "AbsStockT2Unit"):
			directives['t2_compute'] = {
				'ingester': 'AbsStockT2Ingester',
				'units': t2_stock_units
			}

		if t2_point_units := self.get_t2_units(units_config, "AbsDataPointT2Unit"):
			directives['t0_add']['t2_compute'] = {
				'ingester': 'AbsDataPointT2Ingester',
				'units': t2_point_units
			}

		return ret


	def get_t2_units(self, units_config: Dict[str, Any], abs_unit: str) -> List[Dict]:

		return [
			el.dict(skip_defaults=True, by_alias=True)
			for el in self.t2_compute
			if abs_unit in units_config[el.unit]['abc']
		]
