#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/t3/complement/FritzReport.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 03.11.2020
# Date              : 03.11.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import asyncio
from typing import Any, Dict, Iterable, Optional, Tuple

import nest_asyncio
from pydantic.tools import parse_obj_as

from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.abstract.Secret import Secret
from ampel.abstract.AbsBufferComplement import AbsBufferComplement
from ampel.ztf.t3.skyportal.SkyPortalClient import (
    BaseHttpUrl,
    SkyPortalAPIError,
    SkyPortalClient,
)


class FritzReport(SkyPortalClient, AbsBufferComplement):
    """
    Add source record from SkyPortal
    """

    #: Base URL of SkyPortal server
    base_url: BaseHttpUrl = parse_obj_as(BaseHttpUrl, "https://fritz.science")
    #: API token
    token: Secret[str] = {"key": "fritz/jno/ampelbot"}  # type: ignore[assignment]

    async def get_catalog_item(
        self, names: Tuple[str, ...]
    ) -> Optional[Dict[str, Any]]:
        """Get catalog entry associated with the stock name"""
        for name in names:
            if name.startswith("ZTF"):
                try:
                    record = await self.get(f"sources/{name}")
                except SkyPortalAPIError:
                    return None
                # strip out Fritz chatter
                return {
                    k: v
                    for k, v in record["data"].items()
                    if not k in {"thumbnails", "annotations", "groups", "internal_key"}
                }
        return None

    async def update_record(self, record: AmpelBuffer) -> None:
        if (stock := record["stock"]) is None:
            raise ValueError(f"{type(self).__name__} requires stock records")
        item = await self.get_catalog_item(
            tuple(name for name in (stock["name"] or []) if isinstance(name, str))
        )
        if record.get("extra") is None or record["extra"] is None:
            record["extra"] = {self.__class__.__name__: item}
        else:
            record["extra"][self.__class__.__name__] = item

    async def update_records(self, records: Iterable[AmpelBuffer]) -> None:
        async with self.session():
            await asyncio.gather(*[self.update_record(record) for record in records])

    def complement(self, records: Iterable[AmpelBuffer]) -> None:
        # Patch event loop to be reentrant if it is already running, e.g.
        # within a notebook
        try:
            if asyncio.get_event_loop().is_running():
                nest_asyncio.apply()
        except RuntimeError:
            # second call raises: RuntimeError: There is no current event loop in thread 'MainThread'.
            ...
        asyncio.run(self.update_records(records))
