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

        # Categorize buoys into insert, update, and no-op
        insert_buoys = set()
        update_buoys = set()
        noop_buoys = set()

        er_subjects = self._er_client.get_er_subjects()

        # Create maps of ER subjects by serial number
        er_subject_mapping = {subject.get("name"): subject for subject in er_subjects}
        er_subject_names = set(er_subject_mapping.keys())

        # ? Which fields to use to check if the buoy is equivalent to the ER subject
        # Categorize buoys into insert, update, and no-op
        insert_buoys = set()
        update_buoys = set()
        noop_buoys = set()

        observations = []

        for serial_number, buoy_state in buoy_states.items():
            if serial_number in er_subject_names:
                # Buoy exists in ER, so update it
                update_buoys.add(serial_number)
            else:
                # Buoy does not exist in ER, insert it
                insert_buoys.add(serial_number)

        # Process inserts
        for serial_number in insert_buoys:
            buoy_state = buoy_states[serial_number]
            logger.info(f"Inserting new buoy: {serial_number}")
            try:
                new_observations = self.create_observations(buoy_state, None)
                observations.extend(new_observations)
                self.handle_edgetech_data(serial_number, buoy_state)
            except pydantic.ValidationError as ve:
                logger.exception(
                    "Failed making BuoyEvent for %s. Error: %s",
                    serial_number,
                    ve.json(),
                )

        # Process updates
        for serial_number in update_buoys:
            buoy_state = buoy_states[serial_number]
            er_subject = er_subject_mapping.get(serial_number)

            if self.are_equivalent(er_subject, buoy_state):
                # No-op, data is already up to date
                noop_buoys.add(serial_number)
                logger.info(f"No changes needed for buoy: {serial_number}")
            else:
                # Update ER subject
                logger.info(f"Updating buoy: {serial_number}")
                try:
                    new_observations = self.create_observations(buoy_state, er_subject)
                    observations.extend(new_observations)
                    self.handle_edgetech_data(serial_number, buoy_state)
                except pydantic.ValidationError as ve:
                    logger.exception(
                        "Failed updating BuoyEvent for %s. Error: %s",
                        serial_number,
                        ve.json(),
                    )

        logger.info(f"Inserts: {insert_buoys}")
        logger.info(f"Updates: {update_buoys - noop_buoys}")
        logger.info(f"No-ops: {noop_buoys}")

        return observations
