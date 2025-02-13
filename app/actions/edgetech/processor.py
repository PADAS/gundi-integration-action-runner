import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import pydantic

from app.actions.buoy import BuoyClient
from app.actions.buoy.types import ObservationSubject
from app.actions.edgetech.types import Buoy

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

    def _are_equivalent(self, er_subject: ObservationSubject, buoy: Buoy) -> bool:
        """
        Determine if the ObservationSubject (representing a device observation)
        is equivalent to the Buoy (which contains a CurrentState).
        """
        # (1) Serial Number:
        prefix = "edgetech_"
        if er_subject.name.startswith(prefix):
            obs_serial = er_subject.name[len(prefix) :]
        else:
            obs_serial = er_subject.name

        if obs_serial != buoy.serialNumber:
            return False

        if obs_serial != buoy.currentState.serialNumber:
            return False

        # (2) Active state vs. deletion:
        if er_subject.is_active != (not buoy.currentState.isDeleted):
            return False

        # (3) Compare location:
        if (
            er_subject.last_position
            and er_subject.last_position.geometry
            and er_subject.last_position.geometry.coordinates
        ):
            obs_lon, obs_lat = er_subject.last_position.geometry.coordinates

            if buoy.currentState.latDeg is None or buoy.currentState.lonDeg is None:
                return False

            if (
                obs_lat != buoy.currentState.latDeg
                or obs_lon != buoy.currentState.lonDeg
            ):
                return False
        else:
            if (
                buoy.currentState.latDeg is not None
                or buoy.currentState.lonDeg is not None
            ):
                return False

        # (4) Compare update timestamps.
        if er_subject.updated_at != buoy.currentState.lastUpdated:
            return False

        return True

    async def process(self):
        buoy_states = self._get_newest_buoy_states()

        er_subjects = self._er_client.get_er_subjects()

        # Create maps of ER subjects by serial number
        er_subject_mapping = {subject.get("name"): subject for subject in er_subjects}
        er_subject_names = set(er_subject_mapping.keys())

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
                new_observations = buoy_state.create_observation()
                observations.extend(new_observations)
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
                    new_observations = buoy_state.create_observation()
                    observations.extend(new_observations)
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
