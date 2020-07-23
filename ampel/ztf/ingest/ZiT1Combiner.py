#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/ingest/ZiT1Combiner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.03.2020
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Tuple, Set, Union
from ampel.type import DataPointId, ChannelId
from ampel.content.DataPoint import DataPoint
from ampel.content.Compound import CompoundElement
from ampel.ingest.T1PhotoCombiner import T1PhotoCombiner


class ZiT1Combiner(T1PhotoCombiner):

	require = "channel",

	def __init__(self, **kwargs):

		super().__init__(**kwargs)

		if not self.resource:
			raise ValueError("Resource missing")

		if not self.resource.get('channel'):
			raise ValueError("Resource 'channels' is missing")

		self.chan_config = self.resource.get('channel')


	def gen_sub_entry(self, # type: ignore[override]
		dp: DataPoint, channel_name: ChannelId
	) -> Tuple[Union[DataPointId, CompoundElement], Set[str]]:

		tags = {"ZTF"}
		opts = self.chan_config[channel_name]

		# Photopoint ids are referenced by the key name 'pp'
		# whereas upper limis ids are referenced by the key name 'ul'
		comp_entry: CompoundElement = {'id': dp['_id']}
		if 'magpsf' not in dp['body']:
			tags.add("HAS_UPPER_LIMITS") # just an FYI

		# POLICIES
		#  Photopoint option: check if updated zero point should be used
		if 'useUpdatedZP' in opts['policy'] and "HAS_UPDATED_ZP" in dp['tag']:
			comp_entry['tag'] = [1]
			tags.add("HAS_CUSTOM_POLICY")

		# EXCLUSIONS
		# Check access permission (public / partners)
		if "ZTF_PUB" in dp['tag']:
			tags.add("ZTF_PUB")
			if "ZTF_PUB" not in opts['access']:
				comp_entry['excl'] = "Private"
				tags.add("HAS_DATARIGHT_EXCLUSION")

		# I no longer understand this part
		# Check autocomplete
		# elif opts['auto_complete'] is False and "CREATED_BY_AMPEL" in dp['tag']:
		#	comp_entry['excl'] = "Autocomplete"
		#	tags.add("HAS_EXCLUDED_PPS")
		#	tags.add("HAS_AUTOCOMPLETED_PHOTO")

		# Channel specific photophoint exclusion. dp["excl"] could look like this:
		# ["HU_SN", "HU_GRB"]
		elif "excl" in dp.get("excl", []): # type: ignore
			comp_entry['excl'] = "Manual"
			tags.add("HAS_EXCLUDED_PPS")
			tags.add("MANUAL_EXCLUSION")

		#  Check for superseded
		elif "SUPERSEDED" in dp['tag']:
			comp_entry['excl'] = "Superseded"
			tags.add("HAS_EXCLUDED_PPS")
			tags.add("SUPERSEDED_PPS")

		return comp_entry if len(comp_entry) > 1 else comp_entry['id'], tags
