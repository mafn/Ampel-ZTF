#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/base/CatalogMatchFilter.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 19.03.2021
# Last Modified Date: 19.03.2021
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Literal, Dict, Any, Union, Optional, List, cast

from ampel.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.alert.PhotoAlert import PhotoAlert
from ampel.ztf.base.CatalogMatchUnit import CatalogMatchUnit, ConeSearchRequest
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.StrictModel import StrictModel


class BaseCatalogMatchRequest(StrictModel):
    use: Literal["catsHTM", "extcats"]
    name: str
    rs_arcsec: float


class ExtcatsMatchRequest(BaseCatalogMatchRequest):
    use: Literal["extcats"]
    pre_filter: Optional[Dict[str, Any]]
    post_filter: Optional[Dict[str, Any]]


CatalogMatchRequest = Union[BaseCatalogMatchRequest, ExtcatsMatchRequest]


class CatalogMatchFilter(CatalogMatchUnit, AbsAlertFilter[PhotoAlert]):
    """
    A simple filter that matches candidates with a minimum number of previous
    detections (and the most recent detection from a positive subtraction)
    against a set of catalogs. An alert will be accepted if accept condition is
    either None or evaluates to True, and the rejection condition is either not
    or evaluates to False.
    """

    min_ndet: int

    accept: Optional[
        Union[
            CatalogMatchRequest, AnyOf[CatalogMatchRequest], AllOf[CatalogMatchRequest]
        ]
    ]
    reject: Optional[
        Union[
            CatalogMatchRequest, AnyOf[CatalogMatchRequest], AllOf[CatalogMatchRequest]
        ]
    ]

    # TODO: cache catalog lookups if deeply nested models ever become a thing
    def _evaluate_match(
        self,
        ra: float,
        dec: float,
        selection: Union[
            CatalogMatchRequest, AnyOf[CatalogMatchRequest], AllOf[CatalogMatchRequest]
        ],
    ) -> bool:
        if isinstance(selection, AllOf):
            return all(
                self.cone_search_any(ra, dec, [cast(ConeSearchRequest, r.dict()) for r in selection.all_of])
            )
        elif isinstance(selection, AnyOf):
            # recurse into OR conditions
            if isinstance(selection.any_of, AllOf):
                return all(self._evaluate_match(selection.any_of))
            else:
                return any(
                    self.cone_search_any(ra, dec, [cast(ConeSearchRequest, r.dict()) for r in selection.any_of])
                )
        else:
            return all(self.cone_search_any(ra, dec, [cast(ConeSearchRequest, r.dict()) for r in [selection]]))

    def process(self, alert: PhotoAlert) -> bool:

        # cut on the number of previous detections
        if len(alert.pps) < self.min_ndet:
            return False

        # now consider the last photopoint
        latest = alert.pps[0]

        # check if it a positive subtraction
        if not (
            latest["isdiffpos"]
            and (latest["isdiffpos"] == "t" or latest["isdiffpos"] == "1")
        ):
            self.logger.debug("rejected: 'isdiffpos' is %s", latest["isdiffpos"])
            return False

        latest = alert.pps[0]
        ra, dec = latest["ra"], latest["dec"]
        if self.accept:
            if not self._evaluate_match(ra, dec, self.accept):
                return False
        if self.reject:
            if self._evaluate_match(ra, dec, self.reject):
                return False
        return True
