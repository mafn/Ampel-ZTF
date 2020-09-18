#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/t3/complement/ZTFCutoutImages.py
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 18.09.2020
# Last Modified Date: 18.09.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Iterable

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.core.AmpelContext import AmpelContext
from ampel.model.Secret import Secret
from ampel.t3.complement.AbsT3DataAppender import AbsT3DataAppender
from ampel.ztf.archive.ArchiveDB import ArchiveDB


class ZTFCutoutImages(AbsT3DataAppender):
    """
    Add cutout images from archive database to the most recent detection
    """

    auth: Secret[dict] = {"key": "ztf/archive/reader"}  # type: ignore[assignment]

    def __init__(self, context: AmpelContext, **kwargs) -> None:

        AmpelBaseModel.__init__(self, **kwargs)

        self.archive = ArchiveDB(
            context.config.get(f"resource.ampel-ztf/archive", str),
            connect_args=self.auth.get(),
        )

    def complement(self, records: Iterable[AmpelBuffer]) -> None:
        for record in records:
            if (photopoints := record.get("t0")) is None:
                raise ValueError(f"{type(self).__name__} requires t0 records")
            pps = sorted(
                [pp for pp in photopoints if pp["_id"] > 0],
                key=lambda pp: pp["body"]["jd"],
            )
            if record.get("extra") is None:
                record["extra"] = {}
            assert "cutouts" not in record["extra"]
            candid = pps[-1]["_id"]
            record["extra"]["cutouts"] = {candid: self.archive.get_cutout(candid)}
