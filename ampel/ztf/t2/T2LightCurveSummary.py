#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/t2/T2LightCurveSummary.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 16.12.2020
# Last Modified Date: 16.12.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, ClassVar, Dict, List, Literal, Optional

from ampel.abstract.AbsLightCurveT2Unit import AbsLightCurveT2Unit
from ampel.type import T2UnitResult
from ampel.view.LightCurve import LightCurve


class T2LightCurveSummary(AbsLightCurveT2Unit):
    """
    Calculate summary quantities from the light curve.

    This can be signficantly more efficient than calculating the same
    quantities at T3 level for channels that select only a subset of
    datapoints for each stock.
    """

    ingest: ClassVar[Dict[str, Any]] = {"upper_limits": False}

    #: Fields to extract from the latest candidate
    fields: List[str] = ["drb", "ra", "dec"]

    def run(self, lightcurve: LightCurve) -> T2UnitResult:
        result: Dict[str, Any] = {
            "detections": len(lightcurve.get_photopoints() or []),
        }
        if (pps := lightcurve.get_photopoints()):
            first, latest = pps[0]["body"], pps[-1]["body"]
            result["first_detection"] = first["jd"]
            result["ra_dis"], result["dec_dis"] = first["ra"], first["dec"]

            result["last_detection"] = latest["body"]["jd"]
            for k in self.fields:
                result[k] = latest.get(k)

        return result
