#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/ampel/ztf/t3/skyportal/SkyPortalClient.py
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 16.09.2020
# Last Modified Date: 16.09.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import base64
import gzip
import io
import json
from astropy.time import Time
from collections import defaultdict
from datetime import datetime
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Union,
)
from functools import lru_cache

import numpy as np
import requests
from astropy.io import fits
from matplotlib.colors import Normalize
from matplotlib.figure import Figure

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.Secret import Secret
from ampel.t2.T2RunState import T2RunState

if TYPE_CHECKING:
    from ampel.content.DataPoint import DataPoint
    from ampel.content.T2Record import T2Record
    from ampel.view.TransientView import TransientView


def encode_t2_body(t2: "T2Record") -> str:
    assert t2["body"] is not None
    doc = t2["body"][-1]
    return base64.b64encode(
        json.dumps(
            {
                "timestamp": datetime.fromtimestamp(doc["ts"]).isoformat(),
                **{k: v for k, v in doc.items() if k != "ts"},
            }
        ).encode()
    ).decode()


def decode_t2_body(blob: str) -> Dict[str, Any]:
    doc = json.loads(base64.b64decode(blob.encode()).decode())
    return {"ts": int(datetime.fromisoformat(doc.pop("timestamp")).timestamp()), **doc}


def render_thumbnail(cutout_data: bytes) -> str:
    """
    Render gzipped FITS as base64-encoded PNG
    """
    with gzip.open(io.BytesIO(cutout_data), "rb") as f:
        with fits.open(f) as hdu:
            header = hdu[0].header
            img = np.flipud(hdu[0].data)
    mask = np.isfinite(img)

    fig = Figure(figsize=(1, 1))
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()
    ax.imshow(
        img,
        # clip pixel values below the median
        norm=Normalize(*np.percentile(img[mask], [0.5, 99.5])),
        aspect="auto",
        origin="lower",
    )

    with io.BytesIO() as buf:
        fig.savefig(buf, dpi=img.shape[0])
        return base64.b64encode(buf.getvalue()).decode()


ZTF_FILTERS = {1: "ztfg", 2: "ztfr", 3: "ztfi"}
CUTOUT_TYPES = {"science": "new", "template": "ref", "difference": "sub"}

class SkyPortalAPIError(IOError):
    ...

class SkyPortalClient(AmpelBaseModel):

    #: Base URL of SkyPortal server
    base_url: str = "http://localhost:9000"
    #: API token
    token: Secret[str]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._request_kwargs = {
            "headers": {"Authorization": f"token {self.token.get()}"}
        }
        self._session = requests.Session()

    def request(self, verb, endpoint, raise_exc=True, _decode_json=True, **kwargs):
        if endpoint.startswith("/"):
            url = self.base_url + endpoint
        else:
            url = self.base_url + "/api/" + endpoint
        response = self._session.request(
            verb, url, **{**self._request_kwargs, **kwargs}
        )
        if _decode_json:
            payload = response.json()
            if raise_exc and payload["status"] != "success":
                raise SkyPortalAPIError(payload["message"])
            return payload
        else:
            return response

    def get_id(self, endpoint, params, default=None):
        """Query for an object by id, inserting it if not found"""
        if not (response := self.get(endpoint, params=params, raise_exc=False))["data"]:
            response = self.post(endpoint, json=default or params)
        if isinstance(response["data"], list):
            return response["data"][0]["id"]
        else:
            return response["data"]["id"]

    @lru_cache(1024)
    def get_by_name(self, endpoint, name):
        try:
            return next(
                d["id"]
                for d in self.get(endpoint, params={"name": name})["data"]
                if d["name"] == name
            )
        except StopIteration:
            pass
        raise KeyError(f"No {endpoint} named {name}")

    def get(self, endpoint, **kwargs):
        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint, **kwargs):
        return self.request("POST", endpoint, **kwargs)

    def put(self, endpoint, **kwargs):
        return self.request("PUT", endpoint, **kwargs)

    def head(self, endpoint, **kwargs):
        return self.request("HEAD", endpoint, _decode_json=False, **kwargs)


class FilterGroupProvisioner(SkyPortalClient):
    """
    Set up filters to corresponding to AMPEL channels
    """

    #: mapping from ampel stream name to Fritz stream name
    stream_names: Dict[str, str] = {
        "ztf_uw_public": "ZTF Public",
        "ztf_uw_private": "ZTF Public+Partnership",
        "ztf_uw_caltech": "ZTF Public+Partnership+Caltech",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_filter(self, name, stream, group):
        try:
            return self.get_by_name("filters", name)
        except KeyError:
            ...
        doc = {
            "name": name,
            "stream_id": self.get_by_name("streams", self.stream_names[stream]),
            "group_id": self.get_by_name("groups", group),
        }
        self.post("filters", json=doc)
        return self.get_by_name("filters", name)

    def create_filters(
        self, config: "AmpelConfig", group: str, stream: Optional[str] = None
    ) -> None:
        """
        Create a dummy SkyPortal filter for each Ampel filter
        
        :param group: name of group that should own the filter
        :param stream:
          name of the filter's alert stream (meaningless, since there is no
          filter actually defined)
        """
        for channel in config.get("channel").values():
            if not channel.get('active', True):
                continue
            name = f"AMPEL.{channel['channel']}"
            try:
                self.get_by_name("filters", name)
                continue
            except KeyError:
                ...
            self.create_filter(name, stream or channel["template"], group)


def provision_seed_data(client: SkyPortalClient):
    """Set up instruments and groups for a test instance"""
    p48 = client.get_id(
        "telescope",
        {"name": "P48"},
        {
            "diameter": 1.2,
            "elevation": 1870.0,
            "lat": 33.3633675,
            "lon": -116.8361345,
            "nickname": "Palomar 1.2m Oschin",
            "name": "P48",
            "skycam_link": "http://bianca.palomar.caltech.edu/images/allsky/AllSkyCurrentImage.JPG",
            "robotic": True,
        },
    )

    source = {
        "instrument": client.get_id(
            "instrument",
            {"name": "ZTF"},
            {
                "filters": ["ztfg", "ztfr", "ztfi"],
                "type": "imager",
                "band": "optical",
                "telescope_id": p48,
                "name": "ZTF",
            },
        ),
        "stream": client.get_id("streams", {"name": "ztf_partnership"}),
        "group": 1,  # root group
    }
    if not source["stream"] in [
        groupstream["id"]
        for groupstream in client.get(f"groups/{source['group']}")["data"]["streams"]
    ]:
        client.post(
            f"groups/{source['group']}/streams", json={"stream_id": source["stream"]}
        )
    source["filter"] = client.get_id(
        "filters",
        {"name": "highlander"},
        {
            "name": "highlander",
            "stream_id": source["stream"],
            "group_id": source["group"],
        },
    )

    # ensure that all users are in the root group
    users = [
        users["username"]
        for users in client.get(f"groups/{source['group']}")["data"]["users"]
    ]
    for user in client.get("user")["data"]:
        if not user["username"] in users:
            client.post(
                f"groups/{source['group']}/users", json={"username": user["username"]},
            )

    return source


class BaseSkyPortalPublisher(SkyPortalClient):

    logger: AmpelLogger

    def __init__(self, **kwargs):
        if not "logger" in kwargs:
            kwargs["logger"] = AmpelLogger.get_logger()
        super().__init__(**kwargs)

        self.groups = {
            doc["name"]: doc["id"]
            for doc in self.get("groups", raise_exc=True)["data"][
                "user_accessible_groups"
            ]
        }

    def _transform_datapoints(
        self, dps: Sequence["DataPoint"], after=-float("inf")
    ) -> Generator[Dict[str, Any], None, None]:
        for dp in dps:
            body = dp["body"]
            if body["jd"] <= after:
                continue
            base = {
                "_id": dp["_id"],
                "filter": ZTF_FILTERS[body["fid"]],
                "mjd": body["jd"] - 2400000.5,
                "limiting_mag": body["diffmaglim"],
            }
            if body.get("magpsf") is not None:
                content = {
                    "mag": body["magpsf"],
                    "magerr": body["sigmapsf"],
                    "ra": body["ra"],
                    "dec": body["dec"],
                }
            else:
                content = {k: None for k in ("mag", "magerr", "ra", "dec")}
            yield {**base, **content}

    def make_photometry(
        self, datapoints: Sequence["DataPoint"], after=-float("inf")
    ) -> Dict[str, Any]:
        content = defaultdict(list)
        for doc in self._transform_datapoints(datapoints, after):
            for k, v in doc.items():
                content[k].append(v)
        return dict(content)

    def _find_instrument(self, tags: List[str]) -> int:
        for tag in tags:
            try:
                return self.get_by_name("instrument", tag)
            except:
                ...
        raise KeyError(f"None of {tags} match a known instrument")

    def post_candidate(
        self,
        view: "TransientView",
        filters: Optional[List[str]] = None,
        groups: Optional[List[str]] = None,
        instrument: Optional[str] = None,
    ):
        """
        Perform the following actions:
          * Post candidate to filters specified by ``filters``. ``ra``/``dec``
            are taken from the most recent detection; ``drb`` from the maximum
            over detections.
          * Post photometry using the ``candid`` of the latest detection.
          * Post a PNG-encoded cutout of the last detection image.
          * Post each T2 result as comment with a JSON-encoded attachment. If
            a comment corresponding to the T2 unit already exists, overwrite it
            with the most recent result.
        
        :param view:
            Data to post
        :param filters:
            Names of the filter to associate with the candidate. If None, use
            filters named AMPEL.{channel} for each ``channel`` the transient
            belongs to.
        :param groups:
            Names of the filter to associate with the candidate. If None, use
            all accessible groups associated with token.
        :param instrumentname:
            Name of the instrument with which to associate the photometry.
        """

        filter_ids = [
            self.get_by_name("filters", name)
            for name in (
                filters or [f"AMPEL.{channel}" for channel in view.stock["channel"]]
            )
        ]
        group_ids = (
            ([self.get_by_name("groups", name) for name in (groups)])
            if groups
            else "all"
        )
        instrument_id = (
            self.get_by_name("instrument", instrument)
            if instrument
            else self._find_instrument(view.stock["tag"])
        )

        assert view.stock and view.stock["name"] is not None
        name = next(
            n for n in view.stock["name"] if isinstance(n, str) and n.startswith("ZTF")
        )

        # latest lightcurve
        if not view.lightcurve:
            self.logger.warning(f"No light curve found for {name}!")
            return
        lc = sorted(view.lightcurve, key=lambda lc: len(lc.get_photopoints() or []))[-1]
        # detections, in order
        pps = sorted((lc.get_photopoints() or []), key=lambda pp: pp["body"]["jd"])
        dps = sorted(
            pps + list(lc.get_upperlimits() or []), key=lambda pp: pp["body"]["jd"]
        )

        # post transient
        doc = {
            "id": name,
            "ra": pps[-1]["body"]["ra"],
            "dec": pps[-1]["body"]["dec"],
            "ra_dis": pps[0]["body"]["ra"],
            "dec_dis": pps[0]["body"]["dec"],
            "origin": "Ampel",
            "score": max(drb) if (drb := lc.get_values("drb")) is not None else None,
            "detect_photometry_count": len(pps),
            "transient": True,  # sure, why not
        }
        filter_doc = {
            "passing_alert_id": pps[-1]["_id"],
            "passed_at": Time(pps[-1]["body"]["jd"], format="jd").to_datetime().isoformat(),
        }

        if (response := self.get(f"candidates/{name}", raise_exc=False))[
            "status"
        ] == "success":
            # update filters
            new_filters = list(set(filter_ids).difference(response["data"]["filter_ids"]))
            if new_filters:
                self.post(f"candidates", json={"id": name, "filter_ids": new_filters, **filter_doc})
        else:
            self.post("candidates", json={"filter_ids": filter_ids, **doc, **filter_doc})

        # Post photometry for the latest light curve.
        # For ZTF, the id of the most recent detection is the alert id
        # NB: we rely on the advertised, but as of 2020-09-17, unimplemented,
        # feature that repeated POST /api/photometry with the same alert_id and
        # group_ids are idempotent
        photometry = {
            "obj_id": name,
            "group_ids": [1], # change to "all" after https://github.com/skyportal/skyportal/issues/1263 is fixed
            "magsys": "ab",
            "instrument_id": self.get_by_name("instrument", "ZTF"),
            **self.make_photometry(dps),
        }
        datapoint_ids = photometry.pop("_id")
        photometry_response = self.put("photometry", json=photometry)
        photometry_ids = photometry_response["data"]["ids"]
        assert len(datapoint_ids) == len(photometry_ids)
        # TODO: stash bulk upload id somewhere for reference
        photometry_response["data"].get("upload_id")

        for candid, cutouts in (view.extra or {}).get("ZTFCutoutImages", {}).items():
            photometry_id = photometry_ids[datapoint_ids.index(candid)]
            for kind, blob in cutouts.items():
                assert isinstance(blob, bytes)
                # FIXME: switch back to FITS when SkyPortal supports it
                self.post(
                    "thumbnail",
                    json={
                        "photometry_id": photometry_id,
                        "data": render_thumbnail(blob),
                        "ttype": CUTOUT_TYPES[kind],
                    },
                    raise_exc=True,
                )

        # represent latest T2 results as a comments
        latest_t2: Dict[str, "T2Record"] = {}
        for t2 in view.t2 or []:
            if t2["status"] != T2RunState.COMPLETED or not t2["body"]:
                continue
            assert isinstance(t2["unit"], str)
            if t2["unit"] not in latest_t2 or latest_t2[t2["unit"]]["_id"] < t2["_id"]:
                latest_t2[t2["unit"]] = t2
        previous_comments = response["data"]["comments"] if response["status"] == "success" else []
        for t2 in latest_t2.values():
            # find associated comment
            for comment in previous_comments:
                if comment["text"] == t2["unit"]:
                    break
            else:
                # post new comment
                self.logger.debug(f"posting {t2['unit']}")
                self.post(
                    "comment",
                    json={
                        "obj_id": name,
                        "text": t2["unit"],
                        "attachment": {
                            "body": encode_t2_body(t2),
                            "name": f"{name}.{t2['unit']}.json",
                        },
                    },
                )
                continue
            # update previous comment
            previous_body = decode_t2_body(comment["attachment_bytes"])
            if (t2["body"] is not None) and (
                t2["body"][-1]["ts"] > previous_body["ts"]
            ):
                self.logger.debug(f"updating {t2['unit']}")
                self.request(
                    "PUT",
                    f"comment/{comment['id']}",
                    json={
                        "attachment_bytes": encode_t2_body(t2),
                        "author_id": comment["author_id"],
                        "obj_id": name,
                        "text": comment["text"],
                    },
                )
            else:
                self.logger.debug(f"{t2['unit']} exists and is current")
        return
