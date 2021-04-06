#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File				: Ampel-ZTF/ampel/model/ZTFLegacyChannelTemplate.py
# License			: BSD-3-Clause
# Author			: vb <vbrinnel@physik.hu-berlin.de>
# Date				: 16.10.2019
# Last Modified Date: 11.08.2020
# Last Modified By	: Jakob van Santen <jakob.van.santen@desy.de>

from typing import Dict, Any, ClassVar, List, Union
from pydantic import validator
from ampel.log.AmpelLogger import AmpelLogger
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.model.template.AbsLegacyChannelTemplate import AbsLegacyChannelTemplate, T2UnitModel
from ampel.model.StrictModel import StrictModel
from ampel.model.UnitModel import UnitModel

class LegacyT2ComputeModel(StrictModel):
	#: run these units on alerts from the stream
	alerts: List[T2UnitModel] = []
	#: run these units on archival light curves
	archive: List[T2UnitModel] = []

class ZTFLegacyChannelTemplate(AbsLegacyChannelTemplate):
	"""
	Channel template for ZTF. Each of the named variants consumes adifferent
	alert streams from IPAC, and produce stocks with a different set of tags:
	
	============== ============== ========================
	Template       ZTF programids Tags
	============== ============== ========================
	ztf_uw_private 1, 2, 3_public ZTF, ZTF_PUB, ZTF_COLLAB
	ztf_uw_public  1, 3_public    ZTF, ZTF_PUB
	============== ============== ========================
	"""

	# static variables (ClassVar type) are ignored by pydantic
	_access: ClassVar[Dict[str,List[str]]] = {
		"ztf_uw_private": ["ZTF", "ZTF_PUB", "ZTF_COLLAB"],
		"ztf_uw_public": ["ZTF", "ZTF_PUB"],
		"ztf_uw_caltech": ["ZTF", "ZTF_PUB"]
	}

	#: T2 units to trigger when transient is updated
	t2_compute: Union[List[T2UnitModel], LegacyT2ComputeModel] = LegacyT2ComputeModel(alerts=[T2UnitModel(unit="T2LightCurveSummary")]) # type: ignore[assignment]

	# prevent validator from wrapping LegacyT2ComputeModel in list
	@validator('t2_compute', pre=True, each_item=False)
	def cast_to_list_if_required(cls, v):
		if isinstance(v, dict) and "unit" in v:
			return [v]
		return v

	# Mandatory implementation
	def get_channel(self, logger: AmpelLogger) -> Dict[str, Any]:
		assert self.template is not None
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


		t0_ingester = "ZiAlertContentIngester"
		t1_ingester = ("PhotoCompoundIngester", {"combiner": {"unit": "ZiT1Combiner"}})
		# Add a T2LightCurveSummary if it does not already exist
		t2_compute_from_t0 = self.t2_compute.alerts if isinstance(self.t2_compute, LegacyT2ComputeModel) else self.t2_compute
		t2_compute_from_t1 = self.t2_compute.archive if isinstance(self.t2_compute, LegacyT2ComputeModel) else []
		if not any(model.unit == "T2LightCurveSummary" for model in t2_compute_from_t0):
			t2_compute_from_t0.append(T2UnitModel(unit="T2LightCurveSummary"))
		ret.insert(0,
			self.craft_t0_process(
				first_pass_config,
				controller = {
					"unit": "ZTFAlertStreamController"
				},
				stock_ingester = "ZiStockIngester",
				t0_ingester = t0_ingester,
				t1_ingester = t1_ingester,
				t1_standalone_ingester = (
					"ZiT1ArchivalCompoundIngester",
					{
						"datapoint_ingester": t0_ingester,
						"compound_ingester": {"unit": t1_ingester[0], "config": t1_ingester[1]}
					}
				),
				t2_state_ingester = ("PhotoT2Ingester", {"tags": ["ZTF"]}),
				t2_point_ingester = ("DualPointT2Ingester", {"tags": ["ZTF"]}),
				t2_stock_ingester = ("StockT2Ingester", {"tags": ["ZTF"]}),
				t2_compute_from_t0 = t2_compute_from_t0,
				t2_compute_from_t1 = t2_compute_from_t1,
			)
		)
		ret[0]["processor"]["config"].update({
			"supplier": {"unit": "ZiAlertSupplier"}
		})

		return ret
