from functools import cached_property
from typing import Any, Dict, List, Sequence, Set, Tuple, Type, Union, Optional
from ampel.abstract.AbsAlertSupplier import AbsAlertSupplier

import backoff
import requests
from requests_toolbelt.sessions import BaseUrlSession

from ampel.abstract.AbsT0Muxer import AbsT0Muxer
from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.alert.PhotoAlert import PhotoAlert
from ampel.content.DataPoint import DataPoint
from ampel.core.UnitLoader import CT
from ampel.abstract.Secret import Secret
from ampel.secret.NamedSecret import NamedSecret
from ampel.model.UnitModel import UnitModel
from ampel.types import ChannelId, StockId
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.ztf.ingest.ZiDataPointShaper import ZiDataPointShaper
from ampel.ztf.util.ZTFIdMapper import to_ztf_id


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        req.headers["authorization"] = f"bearer {self.token}"
        return req


class ZiArchiveMuxer(AbsT0Muxer):
    """
    Add datapoints from archived ZTF-IPAC alerts.
    """

    #: Number of days of history to add, relative to the earliest point in the
    #: t0 collection
    history_days: int

    shaper: Union[UnitModel, str] = "ZiDataPointShaper"
    archive_token: NamedSecret[str] = NamedSecret(label="ztf/archive/token")

    # Standard projection used when checking DB for existing PPS/ULS
    projection: Dict[str, int] = {
        "_id": 1,
        "tag": 1,
        "excl": 1,
        "body.jd": 1,
        "body.fid": 1,
        "body.rcid": 1,
        "body.magpsf": 1,
    }

    def __init__(self, **kwargs) -> None:

        super().__init__(**kwargs)

        self._shaper = self.context.loader.new_logical_unit(
            model=UnitModel(unit="ZiDataPointShaper"),
            logger=self.logger,
            sub_type=AbsT0Unit,
        )

        self._t0_col = self.context.db.get_collection("t0", "w")

    # NB: init lazily, as Secret properties are not resolved until after __init__()
    @cached_property
    def session(self) -> BaseUrlSession:
        session = BaseUrlSession(
            base_url=(
                url
                if (
                    url := self.context.config.get(
                        "resource.ampel-ztf/archive", str, raise_exc=True
                    )
                ).endswith("/")
                else url + "/"
            )
        )
        session.auth = BearerAuth(self.archive_token.get())
        return session

    def get_earliest_jd(
        self, stock_id: StockId, datapoints: Sequence[DataPoint]
    ) -> float:
        """
        return the smaller of:
          - the smallest jd of any photopoint in datapoints
          - the smallest jd of any photopoint in t0 from the same stock
        """
        from_alert = min(
            (
                dp["body"]["jd"]
                for dp in datapoints
                if dp["id"] > 0 and "ZTF" in dp["tag"]
            )
        )
        if (
            from_db := next(
                self._t0_col.aggregate(
                    [
                        {
                            "$match": {
                                "id": {"$gt": 0},
                                "stock": stock_id,
                                "body.jd": {"$lt": from_alert},
                                "tag": "ZTF",
                            }
                        },
                        {"$group": {"_id": None, "jd": {"$min": "$body.jd"}}},
                    ]
                ),
                {"jd": None},
            )["jd"]
        ) is None:
            return from_alert
        else:
            return min((from_alert, from_db))

    @backoff.on_exception(
        backoff.expo,
        requests.HTTPError,
        giveup=lambda e: e.response.status_code not in {503, 504, 429, 408},
        max_time=600,
    )
    def get_photopoints(self, ztf_name: str, before_jd: float) -> list[dict[str, Any]]:
        response = self.session.get(
            f"object/{ztf_name}/photopoints",
            params={"jd_end": before_jd, "jd_start": before_jd - self.history_days},
        )
        response.raise_for_status()
        return response.json()

    def process(
        self, dps: List[DataPoint], stock_id: Optional[StockId] = None
    ) -> Tuple[Optional[List[DataPoint]], Optional[List[DataPoint]]]:
        """
        :param dps: datapoints from alert
        :param stock_id: stock id from alert
        Attempt to determine which pps/uls should be inserted into the t0 collection,
        and which one should be marked as superseded.
        """
        # Find photopoints from earlier alerts
        if stock_id is None or not (
            history := self.get_photopoints(
                to_ztf_id(stock_id),
                before_jd=self.get_earliest_jd(to_ztf_id(stock_id), dps),
            )
        ):
            # no new points to add; use input points for combination
            return dps, dps

        alert = ZiAlertSupplier.shape_alert_dict(history)
        dps_to_insert = self._shaper.process(alert.dps, stock_id)

        extended_dps = sorted(dps_to_insert + dps, key=lambda d: d["body"]["jd"])

        return extended_dps, extended_dps
