
import bson
from datetime import datetime
from typing import Optional, Union, List

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.db.AmpelDB import AmpelDB

class TroublesAlertLoader:
    """
    Recover alerts stashed in the troubles collection
    """

    @staticmethod
    def alerts(limit : int=None, after : Optional[datetime]=None, channels : Optional[List[str]]=None, remove_records : bool=True):
        """
        :param remove_records: remove record once the next item is requested
        """
        col = AmpelDB.get_collection('troubles')
        query = {
            "tier": {"$eq": 0},
            "section": {"$eq": "ap_filter"}
        }
        if after:
            query["_id"] = {"$gte": bson.ObjectId.from_datetime(after)}
        if channels:
            channel_defs = AmpelConfig.get_config('channels')
            query["channel"] = {"$in": [channel_defs[c]['channel'] for c in channels]}
        pipeline = [
            {"$match": query},
            {"$group": {
                "_id": "$alert.id",
                "docIds": {"$addToSet": "$_id"},
                "tranId": {"$last": "$tranId"},
                "alert": {"$last": "$alert"}
            }}
        ]
        previous = None
        try:
            for record in col.aggregate(pipeline, allowDiskUse=True):
                if remove_records and previous is not None:
                    col.delete_many({"_id": {"$in": record['docIds']}})
                    previous = None
                yield record
                previous = record
        finally:
            if remove_records and previous is not None:
                col.delete_many({"_id": {"$in": record['docIds']}})
