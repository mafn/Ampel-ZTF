#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File				: Ampel-ZTF/ampel/model/ZTFLegacyChannelTemplate.py
# License			: BSD-3-Clause
# Author			: vb <vbrinnel@physik.hu-berlin.de>
# Date				: 16.10.2019
# Last Modified Date: 15.04.2020
# Last Modified By	: vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, ClassVar, List, Union, Optional, Sequence, Literal
from ampel.log.AmpelLogger import AmpelLogger
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.model.template.AbsLegacyChannelTemplate import AbsLegacyChannelTemplate

from ampel.model.StrictModel import StrictModel
from ampel.model.t3.T2FilterModel import T2FilterModel
from ampel.model.UnitModel import UnitModel

class FilterModel(StrictModel):
	t2: Union[T2FilterModel, Sequence[T2FilterModel]]

class EmbeddedT3Process(StrictModel):
	name: Optional[str] = None
	template: Literal["periodic_summary"]
	schedule: Union[str,Sequence[str]]
	load: Optional[Sequence[str]] = None
	filter: Optional[FilterModel] = None
	run: Union[UnitModel, Sequence[UnitModel]]

	def get_run(self) -> Sequence[UnitModel]:
		if isinstance(self.run, UnitModel):
			return [self.run]
		else:
			return self.run

	def get_schedule(self) -> Sequence[str]:
		if isinstance(self.schedule, str):
			return [self.schedule]
		else:
			return self.schedule

	def get_t2_filters(self) -> Sequence[T2FilterModel]:
		if not self.filter:
			return []
		elif isinstance(self.filter, FilterModel):
			return [self.filter.t2]
		else:
			return [f.t2 for f in self.filter]

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

	def apply_t3_template(self, template: EmbeddedT3Process, index: int=0) -> Dict[str,Any]:
		name = template.name if template.name else f"summary_{index:02d}"
		process_name = f"{self.channel}|T3|{name}"
		directive : Dict[str, Any] = {
			"select": {
				"unit": "T3StockSelector",
				"config": {
					"modified": {
						"after": {
							"match_type": "time_last_run",
							"process_name": process_name
						}
					},
					"channel": self.channel,
					"tag": {
						"with": "ZTF",
						"without": "HAS_ERROR"
					}
				}
			},
			"load": {
				"unit": "T3SimpleDataLoader",
				"config": {
					"directives": ["TRANSIENT","DATAPOINT","T2RECORD"]
				}
			},
			"run": {
				"unit": "T3UnitRunner",
				"config": {
					"directives": [
						{
							"execute": [
								run.dict() for run in template.get_run()
							]
						}
					]
				}
			}
		}
		# Restrict stock selection according to T2 values
		if t2_filters := template.get_t2_filters():
			directive["select"]["unit"] = "T3FilteringStockSelector"
			directive["select"]["config"]["t2_filter"] = [f.dict() for f in t2_filters]
		# Restrict document types to load
		if template.load:
			directive["load"]["config"]["directives"] = template.load
		
		ret: Dict[str, Any] = {
			"tier": 3,
			"schedule": template.schedule,
			"active": self.active,
			"distrib": self.distrib,
			"source": self.source,
			"channel": self.channel,
			"name": process_name,
			"processor": {
				"unit": "T3Processor",
				"config": {
					"process_name": process_name,
					"directives": [directive]
				}
			}
		}

		return ret

	# Mandatory implementation
	def get_processes(self, logger: AmpelLogger, first_pass_config: FirstPassConfig) -> List[Dict[str, Any]]:

		# T3 processes
		ret: List[Dict[str, Any]] = []

		for index, el in enumerate(self.t3_supervise):
			if el.get("template") == "periodic_summary":
				ret.append(self.apply_t3_template(EmbeddedT3Process(**el), index))
			else:
				# raw channel config
				ret.append(self.transfer_channel_parameters(el))

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
				t2_point_ingester = ("PointT2Ingester", {"tags": ["ZTF"]}),
				t2_stock_ingester = ("StockT2Ingester", {"tags": ["ZTF"]}),
			)
		)

		return ret
