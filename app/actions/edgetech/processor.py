import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import pydantic

from app.actions.buoy import BuoyClient
from app.actions.edgetech.enums import Buoy

logger = logging.getLogger(__name__)

class EdgetTechProcessor:
    def __init__(
        self, data: List[dict], er_token: str, er_url: str, filters: dict = None
    ):
        self._data = [Buoy.parse_obj(record) for record in data]
        self._er_client = BuoyClient(er_token, er_url)
        self._filters = filters or self._get_default_filters()

    def _get_default_filters(self):
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=180)
        return {"start_date": start_date, "end_date": end_date}

    def _filter_data(self):
        filtered_data: List[Buoy] = []
        for record in self._data:
            record_current_state = record.currentState.lastUpdated
            if (
                self._filters["start_date"]
                <= record_current_state.timestamp
                <= self._filters["end_date"]
            ):
                filtered_data.append(record)
        return filtered_data

    def _get_newest_buoy_states(self) -> Dict[str, Buoy]:
        filtered_data = self._filter_data()
        newest_by_serial = {}
        for buoy in filtered_data:
            serial = buoy.currentState.serialNumber
            current_state = buoy.currentState
            current_newest = newest_by_serial.get(serial)
            if (
                serial not in newest_by_serial
            ) or current_state.lastUpdated > current_newest.lastUpdated:
                newest_by_serial[serial] = buoy
        return newest_by_serial

    async def process(self):
        buoy_states = self._get_newest_buoy_states()

        er_subjects = self._er_client.get_er_subjects()

        # TODO: Implement method
