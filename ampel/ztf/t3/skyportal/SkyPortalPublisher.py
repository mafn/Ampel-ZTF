#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/t3/skyportal/SkyPortalPublisher.py
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 16.09.2020
# Last Modified Date: 16.09.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import asyncio
from typing import (
    Dict,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
)

import nest_asyncio

from ampel.abstract.AbsPhotoT3Unit import AbsPhotoT3Unit
from ampel.struct.JournalExtra import JournalExtra
from ampel.t2.T2RunState import T2RunState
from ampel.type import StockId
from ampel.ztf.t3.skyportal.SkyPortalClient import BaseSkyPortalPublisher

if TYPE_CHECKING:
    from ampel.view.TransientView import TransientView


class SkyPortalPublisher(BaseSkyPortalPublisher, AbsPhotoT3Unit):

    #: Number of connections per (host,port) pair
    max_parallel_connections: int = 50

    def add(
        self, tviews: Sequence["TransientView"]
    ) -> Optional[Dict[StockId, JournalExtra]]:
        """Pass each view to :meth:`post_candidate`."""
        # Patch event loop to be reentrant if it is already running, e.g.
        # within a notebook
        if asyncio.get_event_loop().is_running():
            nest_asyncio.apply()
        return asyncio.run(self.post_candidates(tviews))

    async def post_candidates(
        self, tviews: Sequence["TransientView"]
    ) -> Dict[StockId, JournalExtra]:
        """Pass each view to :meth:`post_candidate`."""
        async with self.session(limit_per_host=self.max_parallel_connections):
            return dict(
                await asyncio.gather(
                    *[
                        self.post_view(view)
                        for view in tviews
                        if view.stock is not None
                    ],
                )
            )

    async def post_view(self, view: "TransientView") -> Tuple[StockId, JournalExtra]:
        return (
            view.id,
            JournalExtra(extra=dict(await self.post_candidate(view))),
        )

    def done(self) -> None:
        ...
