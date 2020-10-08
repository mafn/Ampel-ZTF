#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/t3/skyportal/SkyPortalPublisher.py
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 16.09.2020
# Last Modified Date: 16.09.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import (
    Dict,
    Optional,
    Sequence,
    TYPE_CHECKING,
)

from ampel.abstract.AbsPhotoT3Unit import AbsPhotoT3Unit
from ampel.struct.JournalExtra import JournalExtra
from ampel.t2.T2RunState import T2RunState
from ampel.type import StockId
from ampel.ztf.t3.skyportal.SkyPortalClient import BaseSkyPortalPublisher

if TYPE_CHECKING:
    from ampel.view.TransientView import TransientView


class SkyPortalPublisher(BaseSkyPortalPublisher, AbsPhotoT3Unit):
    def add(
        self, tviews: Sequence["TransientView"]
    ) -> Optional[Dict[StockId, JournalExtra]]:
        """Pass each view to :meth:`post_candidate`."""
        for view in tviews:
            self.post_candidate(view)
        return None

    def done(self) -> None:
        ...
