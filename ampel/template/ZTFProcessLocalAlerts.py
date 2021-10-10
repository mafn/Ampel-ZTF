#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/template/ZTFProcessLocalAlerts.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.07.2021
# Last Modified Date: 04.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Any, Literal
from ampel.types import ChannelId
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T2Compute import T2Compute
from ampel.abstract.AbsProcessorTemplate import AbsProcessorTemplate
from ampel.template.AbsEasyChannelTemplate import AbsEasyChannelTemplate


class ZTFProcessLocalAlerts(AbsProcessorTemplate):
	"""
	Returns adequate config for an alert consumer configured to process local alerts
	"""

	channel: ChannelId

	folder: str

	extension: Literal['json', 'avro'] = "json"

	supplier: str = 'ZiAlertSupplier'

	loader: str = 'DirAlertLoader'

 	#: T2 units to trigger when transient is updated. Dependencies of tied
	#: units will be added automatically.
	t2_compute: List[T2Compute] = []

	extra: Dict = {}


	# Mandatory override
	def get_model(self, config: Dict[str, Any], logger: AmpelLogger) -> UnitModel:

		return UnitModel(
			unit = 'AlertConsumer',
			config = self.extra | AbsEasyChannelTemplate.craft_t0_processor_config(
				channel = self.channel,
				config = config,
				t2_compute = self.t2_compute,
				supplier = {
					'unit': self.supplier,
					'config': {
						'deserialize': self.extension,
						'loader': {
							'unit': self.loader,
							'config': {
								'folder': self.folder,
								'extension': f'*.{self.extension}'
							}
						}
					}
				},
				shaper = "ZiDataPointShaper",
				combiner = "ZiT1Combiner",
				filter_dict = None,
				muxer = None,
				compiler_opts = {
					'stock': {'id_mapper': 'ZTFIdMapper', 'tag': 'ZTF'},
					't0': {'tag': 'ZTF'},
					't1': {'tag': 'ZTF'},
					'state_t2': {'tag': 'ZTF'},
					'point_t2': {'tag': 'ZTF'}
				}
			)
		)
