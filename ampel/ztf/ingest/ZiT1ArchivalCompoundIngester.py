from functools import cached_property
from typing import Dict, List, Sequence, Set, Tuple, Type, Union, Optional

import backoff
import requests
from requests_toolbelt.sessions import BaseUrlSession

from ampel.abstract.ingest.AbsAlertContentIngester import AbsAlertContentIngester
from ampel.abstract.ingest.AbsCompoundIngester import AbsCompoundIngester
from ampel.alert.PhotoAlert import PhotoAlert
from ampel.content.DataPoint import DataPoint
from ampel.core.UnitLoader import PT
from ampel.ingest.PhotoCompoundBluePrint import PhotoCompoundBluePrint
from ampel.model.Secret import Secret
from ampel.model.UnitModel import UnitModel
from ampel.type import ChannelId, StockId
from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.ztf.util.ZTFIdMapper import to_ztf_id


class ZiT1ArchivalCompoundIngester(AbsCompoundIngester[PhotoCompoundBluePrint]):
    """
    Ingest data points from archived ZTF-IPAC alerts, and create compounds
    representing the light curve from the start of ZTF operations to the alert
    exposure.
    """

    datapoint_ingester: Union[UnitModel, str]
    compound_ingester: Union[UnitModel, str]
    archive_auth: Secret[dict] = {"key": "ztf/archive/auth"}  # type: ignore[assignment]

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

        self.compound_engine = self._get_ingester(
            self.compound_ingester, AbsCompoundIngester[PhotoCompoundBluePrint]
        )
        self.datapoint_engine = self._get_ingester(
            self.datapoint_ingester, AbsAlertContentIngester[PhotoAlert, DataPoint]
        )

        self._t0_col = self.context.db.get_collection("t0", "w")

        self.session = BaseUrlSession(
            base_url=(
                url
                if (url := self.context.config.get("resource.ampel-ztf/archive", str, raise_exc=True)).endswith("/")
                else url+"/"
            )
        )
        self.session.auth = tuple(self.archive_auth.get().values())

        self.alert_supplier = ZiAlertSupplier(deserialize=None)
        self.channels: Set[ChannelId] = set()

    def _get_ingester(self, model: Union[str, UnitModel], sub_type: Type[PT]) -> PT:
        return self.context.loader.new_admin_unit(
            unit_model=model if isinstance(model, UnitModel) else UnitModel(unit=model),
            context=self.context,
            logd=self.logd,
            updates_buffer=self.updates_buffer,
            run_id=self.run_id,
            sub_type=sub_type,
        )

    def add_channel(self, channel: ChannelId):
        self.channels.add(channel)
        self.compound_engine.add_channel(channel)

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
                if dp["_id"] > 0 and "ZTF" in dp["tag"]
            )
        )
        if (
            from_db := next(
                self._t0_col.aggregate(
                    [
                        {
                            "$match": {
                                "_id": {"$gt": 0},
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
    def get_photopoints(self, ztf_name: str, before_jd: float):
        response = self.session.get(
            f"object/{ztf_name}/photopoints",
            params={"jd_end": before_jd}
        )
        response.raise_for_status()
        return response.json()

    def ingest_previous_alerts(
        self, stock_id: StockId, datapoints: Sequence[DataPoint]
    ) -> None:
        # Ingest photopoints from earlier alerts
        if not (
            history := self.get_photopoints(
                to_ztf_id(stock_id),
                before_jd=self.get_earliest_jd(stock_id, datapoints)
            )
        ):
            return
        self.alert_supplier.set_alert_source(iter([history]))
        # FIXME: do some logd magic to record these in the journal
        for alert in self.alert_supplier:
            self.datapoint_engine.ingest(alert)

    def ingest(
        self,
        stock_id: StockId,
        datapoints: Sequence[DataPoint],
        chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
    ) -> Optional[PhotoCompoundBluePrint]:

        # Keep only channels that requested extended states
        if not (chans := [(k, v) for k, v in chan_selection if k in self.channels]):
            return None

        self.ingest_previous_alerts(stock_id, datapoints)

        # Extract all datapoints for this stock (including superseded ones)
        extended_datapoints: List[DataPoint] = list(
            self._t0_col.find(
                {
                    "stock": stock_id,
                    "body.jd": {"$lte": datapoints[-1]["body"]["jd"]},
                    "tag": "ZTF",
                },
                self.projection,
            ).sort([("body.jd", 1)])
        )

        # Create compounds
        return self.compound_engine.ingest(stock_id, extended_datapoints, chans)
