#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/t3/complement/AddTNSNames.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 13.12.2018
# Last Modified Date: 10.03.2021
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>


from typing import Iterable, Optional, Dict, Any

from pydantic import Field

from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.t3.complement.AbsT3DataAppender import AbsT3DataAppender
from ampel.ztf.base.CatalogMatchUnit import (
    CatalogMatchAdminUnit,
    ExtendedConeSearchRequest,
)


class TNSNames(CatalogMatchAdminUnit, AbsT3DataAppender):
    """
    Add TNS names to transients.
    """

    search_radius: float = Field(3, description="Matching radius in arcsec")
    include_report: bool = False

    def complement(self, records: Iterable[AmpelBuffer]) -> None:
        for record in records:

            # find the latest T2LightCurveSummary result
            if (summary := self._get_t2_result(record, "T2LightCurveSummary")) is None:
                raise ValueError(
                    f"No T2LightCurveSummary found for stock {str(record['id'])}"
                )
            if (ra := summary.get("ra")) is None:
                raise ValueError(
                    f"No T2LightCurveSummary contains no right ascension for stock {str(record['id'])}"
                )
            if (dec := summary.get("dec")) is None:
                raise ValueError(
                    f"No T2LightCurveSummary contains no declination for stock {str(record['id'])}"
                )
            if not (
                matches := self.cone_search_all(
                    ra,
                    dec,
                    [
                        ExtendedConeSearchRequest(
                            {
                                "name": "TNS",
                                "use": "extcats",
                                "rs_arcsec": self.search_radius,
                                "keys_to_append": None
                                if self.include_report
                                else ["objname"],
                            }
                        )
                    ],
                )[0]
            ):
                continue

            if (stock := record.get("stock", None)) is not None:
                existing_names = (
                    tuple(name) if (name := stock.get("name")) is not None else tuple()
                )
                new_names = tuple(
                    n
                    for item in matches
                    if not (n := "TNS" + item["body"]["objname"]) in existing_names
                )
                stock["name"] = existing_names + new_names

            if self.include_report:
                reports = [item["body"] for item in matches]
                if record.get("extra") is None or record["extra"] is None:
                    record["extra"] = {"TNSReports": reports}
                else:
                    record["extra"]["TNSReports"] = reports

    def _get_t2_result(
        self, record: AmpelBuffer, unit_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get the result of the latest invocation of the given unit
        """
        if (t2 := record.get("t2")) is None:
            raise ValueError(f"{type(self).__name__} requires T2 records be loaded")
        try:
            last_t2_result = next(
                iter(
                    sorted(
                        [
                            sorted_records[0]
                            for doc in t2
                            if (
                                doc["unit"] == unit_id
                                and (
                                    sorted_records := sorted(
                                        doc["body"], key=lambda r: r["ts"], reverse=True
                                    )
                                )
                            )
                        ],
                        key=lambda r: r["ts"],
                        reverse=True,
                    )
                )
            )["result"]
        except StopIteration:
            return None
        return (
            last_t2_result[-1] if isinstance(last_t2_result, list) else last_t2_result
        )
