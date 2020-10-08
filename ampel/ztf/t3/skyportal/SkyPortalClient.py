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

    def request(self, verb, endpoint, raise_exc=True, **kwargs):
        if endpoint.startswith("/"):
            url = self.base_url + endpoint
        else:
            url = self.base_url + "/api/" + endpoint
        response = self._session.request(
            verb, url, **{**self._request_kwargs, **kwargs}
        ).json()
        if raise_exc and response["status"] != "success":
            raise RuntimeError(response["message"])
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
            return next(d['id'] for d in self.get(endpoint, params={'name': name})['data'] if d['name']==name)
        except StopIteration:
            raise KeyError(f"No {endpoint} named {name}")

    def get(self, endpoint, **kwargs):
        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint, **kwargs):
        return self.request("POST", endpoint, **kwargs)

    def put(self, endpoint, **kwargs):
        return self.request("PUT", endpoint, **kwargs)


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
    for user in client.get("user")["data"]:
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

    def post_candidate(self, view: "TransientView", filters: Optional[List[str]] = None):
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
        """

        filter_ids = (
            [self.get_by_name("filters", name) for name in (filters or [f"AMPEL.{channel}" for channel in view.stock["channel"]])]
        )

        assert view.stock and view.stock["name"] is not None
        name = next(
            n for n in view.stock["name"] if isinstance(n, str) and n.startswith("ZTF")
        )

        # latest lightcurve
        assert view.lightcurve
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
            "transient": True,  # sure, why not,
        }

        if (response := self.get(f"candidates/{name}", raise_exc=False))[
            "status"
        ] == "success":
            source_id = response["data"]["id"]
            # update if required
            if diff := {k: doc[k] for k in doc if doc[k] != response["data"][k]}:
                self.request("PUT", f"candidates/{name}", json={"filter_ids": filter_ids, **diff})
        else:
            source_id = self.post("candidates", json={"filter_ids": filter_ids, **diff})[
                "data"
            ]["id"]

        # Post photometry for the latest light curve.
        # For ZTF, the id of the most recent detection is the alert id
        # NB: we rely on the advertised, but as of 2020-09-17, unimplemented,
        # feature that repeated POST /api/photometry with the same alert_id and
        # group_ids are idempotent
        photometry = {
            "obj_id": name,
            "alert_id": pps[-1]["_id"],  # candid for ZTF alerts
            "group_ids": group_ids,
            "magsys": "ab",
            "instrument_id": self.instrument_id,
            **self.make_photometry(dps),
        }
        datapoint_ids = photometry.pop("_id")
        photometry_response = self.post("photometry", json=photometry)
        photometry_ids = photometry_response["data"]["ids"]
        assert len(datapoint_ids) == len(photometry_ids)
        # TODO: stash bulk upload id somewhere for reference
        photometry_response["data"]["upload_id"]

        for candid, cutouts in (view.extra or {}).get("cutouts", {}).items():
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
        for t2 in latest_t2.values():
            # find associated comment
            for comment in response["data"]["comments"]:
                if comment["text"] == t2["unit"]:
                    break
            else:
                # post new comment
                self.logger.debug(f"posting {t2['unit']}")
                self.post(
                    "comment",
                    json={
                        "obj_id": source_id,
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
