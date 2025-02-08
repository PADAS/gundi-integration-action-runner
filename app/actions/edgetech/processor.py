from datetime import datetime, timedelta, timezone
from typing import List

from actions.edgetech.enums import Buoy


class EdgetTechProcessor:
    def __init__(self, data: List[dict], filters: dict = None):
        self._data = [Buoy.parse_obj(record) for record in data]
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

    def _get_newest_buoy_states(self) -> List[Buoy]:
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
        return list(newest_by_serial.values())

    def process(self):
        buoy_states = self._get_newest_buoy_states()

        to_resolve = {
            buoy
            for buoy in buoy_states
            if buoy.currentState.isDeleted or not buoy.currentState.isDeployed
        }
        to_deploy = {
            buoy
            for buoy in buoy_states
            if not buoy.currentState.isDeleted and buoy.currentState.isDeployed
        }
