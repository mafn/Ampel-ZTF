import logging
from typing import Dict, Any

import backoff
import requests

from ampel.base.AmpelBaseModel import AmpelBaseModel

log = logging.getLogger(__name__)

class ZTFArchiveAlertLoader(AmpelBaseModel):
    #: Base URL of archive service
    archive: str = "https://ampel.zeuthen.desy.de/api/ztf/archive/v2"
    #: A stream identifier, created via POST /api/ztf/archive/streams/
    stream: str

    def __iter__(self):
        return self.get_alerts()
    
    def get_alerts(self):
        with requests.Session() as session:
            while True:
                chunk = self._get_chunk(session)
                try:
                    yield from chunk["alerts"]
                except GeneratorExit:
                    log.error(
                        f"Chunk from stream {self.stream} partially consumed. "
                        f"Ensure that iter_max is a multiple of the chunk size."
                    )
                    raise
                if len(chunk["alerts"]) == 0 and chunk["chunks_remaining"] == 0:
                    break

    @backoff.on_exception(
        backoff.expo,
        requests.HTTPError,
        giveup=lambda e: e.response.status_code not in {503, 504, 429, 408},
        max_time=600,
    )
    def _get_chunk(self, session: requests.Session) -> Dict[str, Any]:
        response = session.get(f"{self.archive}/stream/{self.stream}/chunk")
        response.raise_for_status()
        return response.json()