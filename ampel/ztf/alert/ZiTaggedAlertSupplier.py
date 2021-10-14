#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/alert/ZiTaggedAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.10.2021
# Last Modified Date: 11.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal, Union, Callable, Any, Dict
from ampel.alert.PhotoAlert import PhotoAlert
from ampel.alert.BaseAlertSupplier import BaseAlertSupplier
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier


class ZiTaggedAlertSupplier(BaseAlertSupplier[PhotoAlert]):
	"""
	Iterable class that, for each alert payload provided by the underlying alert_loader,
	returns an PhotoAlert instance.
	"""

	# Override default
	deserialize: Union[None, Literal["avro", "json"], Callable[[Any], Dict]] = "avro"

	def __next__(self) -> PhotoAlert:
		"""
		:returns: a dict with a structure that AlertConsumer understands
		:raises StopIteration: when alert_loader dries out.
		:raises AttributeError: if alert_loader was not set properly before this method is called
		"""
		d, tag = next(self.alert_loader)
		return ZiAlertSupplier.shape_alert_dict(self.deserialize(d), tag)
