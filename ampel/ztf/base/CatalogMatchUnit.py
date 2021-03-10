#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/base/CatalogMatchUnit.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.03.2021
# Last Modified Date: 10.03.2021
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from functools import cached_property
from typing import Sequence, Dict, Any, Literal, TypedDict, Optional, List, Union, overload

import requests

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.StrictModel import StrictModel

class ConeSearchRequest(TypedDict):
    """
    :param use: either extcats or catsHTM, depending on how the catalog is set up.
    :param rs_arcsec: search radius for the cone search, in arcseconds

    In case 'use' is set to 'extcats', 'catq_kwargs' can (or MUST?) contain the names of the ra and dec
    keys in the catalog (see example below), all valid arguments to extcats.CatalogQuert.findclosest
    can be given, such as pre- and post cone-search query filters can be passed.

    In case 'use' is set to 'catsHTM', 'catq_kwargs' SHOULD contain the the names of the ra and dec
    keys in the catalog if those are different from 'ra' and 'dec' the 'keys_to_append' parameters
    is OPTIONAL and specifies which fields from the catalog should be returned in case of positional match:

    if not present: all the fields in the given catalog will be returned.
    if `list`: just take this subset of fields.

    Example (SDSS_spec):
    {
        'use': 'extcats',
        'catq_kwargs': {
            'ra_key': 'ra',
            'dec_key': 'dec'
        },
        'rs_arcsec': 3,
        'keys_to_append': ['z', 'bptclass', 'subclass']
    }

    Example (NED):
    {
        'use': 'catsHTM',
        'rs_arcsec': 20,
        'keys_to_append': ['fuffa1', 'fuffa2', ..],
    }
    """

    name: str
    use: Literal["extcats", "catsHTM"]
    rs_arcsec: float


class ExtendedConeSearchRequest(ConeSearchRequest, total=False):
    keys_to_append: Optional[Sequence[str]]
    pre_filter: Dict[str, Any]
    post_filter: Dict[str, Any]


class CatalogItem(TypedDict):
    body: Dict[str, Any]
    dist_arcsec: float


class CatalogMatchUnit(AmpelBaseModel):
    """
    A mixin providing catalog matching with catalogmatch-service
    """

    catalogmatch_service: str = "https://ampel.zeuthen.desy.de/api/catalogmatch"

    @cached_property
    def session(self):
        return requests.Session()

    @overload
    def _cone_search(
        self, method: Literal["any"], ra: float, dec: float, catalogs: Sequence[ConeSearchRequest]
    ) -> List[bool]:
        ...
    
    @overload
    def _cone_search(
        self, method: Literal["nearest"], ra: float, dec: float, catalogs: Sequence[ConeSearchRequest]
    ) -> List[Optional[CatalogItem]]:
        ...
    
    @overload
    def _cone_search(
        self, method: Literal["all"], ra: float, dec: float, catalogs: Sequence[ConeSearchRequest]
    ) -> List[Optional[List[CatalogItem]]]:
        ...

    def _cone_search(
        self, method: Literal["any", "nearest", "all"], ra: float, dec: float, catalogs: Sequence[ConeSearchRequest]
    ) -> Union[List[bool], List[Optional[CatalogItem]], List[Optional[List[CatalogItem]]]]:
        response = self.session.post(
            self.catalogmatch_service + f"/cone_search/{method}",
            json={
                "ra_deg": ra,
                "dec_deg": dec,
                "catalogs": catalogs,
            },
        )
        response.raise_for_status()
        return response.json()

    def cone_search_any(
        self, ra: float, dec: float, catalogs: Sequence[ConeSearchRequest]
    ) -> List[bool]:
        return self._cone_search("any", ra, dec, catalogs)

    def cone_search_nearest(
        self, ra: float, dec: float, catalogs: Sequence[ConeSearchRequest]
    ) -> List[Optional[CatalogItem]]:
        return self._cone_search("nearest", ra, dec, catalogs)

    def cone_search_all(
        self, ra: float, dec: float, catalogs: Sequence[ConeSearchRequest]
    ) -> List[Optional[List[CatalogItem]]]:
        return self._cone_search("all", ra, dec, catalogs)
