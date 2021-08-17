#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/PeriodicSummaryT3.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.08.2020
# Last Modified Date: 10.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Dict, Sequence, Optional, Union

from ampel.template.PeriodicSummaryT3 import PeriodicSummaryT3, LoaderDirective


class ZTFPeriodicSummaryT3(PeriodicSummaryT3):
    """
    Periodic summary process with sensible defaults for ZTF.
    """

    tag: Dict = {"with": "ZTF", "without": "HAS_ERROR"}
    load: Optional[Sequence[Union[str, LoaderDirective]]] = ["TRANSIENT", "T2RECORD"]
