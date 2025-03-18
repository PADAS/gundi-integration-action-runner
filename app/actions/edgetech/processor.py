import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pydantic

from app.actions.buoy import BuoyClient
from app.actions.edgetech.types import Buoy

logger = logging.getLogger(__name__)


class EdgetTechProcessor:
    def __init__(
        self,
        data: List[Buoy],
        er_token: str,
        er_url: str,
        filters: Optional[dict] = None,
    ):
        """
        Initialize an EdgetTechProcessor instance.

        This constructor parses raw data records into Buoy objects, initializes an ER client for
        further processing, and sets up filters for processing buoy data.

        Args:
            data (List[Buoy]): A list of Buoy objects (parsed from raw data).
            er_token (str): Authentication token for the ER client.
            er_url (str): URL endpoint for the ER client.
            filters (dict, optional): A dictionary containing filter criteria (e.g., start_date and end_date).
                                      If not provided, default filters covering the last 180 days are used.
        """
        self._data = [Buoy.parse_obj(record) for record in data]
        self._er_client = BuoyClient(er_token, er_url)
        self._filters = filters or self._get_default_filters()
        self._prefix = "edgetech_"

    def _get_default_filters(self) -> Dict[str, Any]:
        """
        Generate default filter criteria for processing buoy data.

        By default, this method defines a time window starting 180 days before the current UTC time
        and ending at the current UTC time.

        Returns:
            Dict[str, Any]: A dictionary with 'start_date' and 'end_date' keys defining the filter window.
        """
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=180)
        return {"start_date": start_date, "end_date": end_date}

    def _filter_data(self) -> List[Buoy]:
        """
        Filter buoy data records based on the time window defined in self._filters.

        Iterates over the parsed buoy data and selects records whose currentState.lastUpdated
        timestamp falls within the filter's start_date and end_date.

        Returns:
            List[Buoy]: A list of Buoy objects that satisfy the filter criteria.
        """
        filtered_data: List[Buoy] = []
        for record in self._data:
            record_current_state = record.currentState.lastUpdated
            if (
                self._filters["start_date"].timestamp()
                <= record_current_state.timestamp()
                <= self._filters["end_date"].timestamp()
            ):
                filtered_data.append(record)
        return filtered_data

    def _group_by_serial_number(self) -> Dict[str, List[Buoy]]:
        """
        Group the filtered Buoy data by serial number, returning a dictionary in which each key
        is a serial number and each value is a list of Buoy records for that serial number.

        The lists are sorted in descending order (most recent first) by each Buoy's
        currentState.lastUpdated.

        Returns:
            Dict[str, List[Buoy]]: A dictionary mapping each serial number to a sorted list of Buoy objects.
        """
        filtered_data = self._filter_data()
        buoy_states: Dict[str, List[Buoy]] = {}

        for record in filtered_data:
            serial_number = record.serialNumber
            if serial_number not in buoy_states:
                buoy_states[serial_number] = [record]
            else:
                buoy_states[serial_number].append(record)

        for serial_number, buoy_list in buoy_states.items():
            buoy_list.sort(key=lambda x: x.currentState.lastUpdated, reverse=True)

        return buoy_states

    async def process(self) -> List[dict]:
        """
        Process buoy data to generate observation events for the ER system.

        This asynchronous method performs the following steps:
            1. Retrieves and filters the latest buoy states grouped by serial number.
            2. Fetches existing ER subjects from the ER client.
            3. Maps ER subjects by name and categorizes buoy states into:
                - Inserts: Buoys not yet present in ER.
                - Updates: Buoys present in ER that need to be updated.
                - No-ops: Buoys in ER with unchanged data.
            4. Creates observation events for new or updated buoys.
            5. Logs information about inserts, updates, and no-ops.

        Returns:
            List[dict]: A list of dictionaries representing the observation events generated during processing.

        Raises:
            pydantic.ValidationError: If validation fails while creating buoy observation events.
        """
        buoy_states_by_serial_number = self._group_by_serial_number()
        er_subjects = await self._er_client.get_er_subjects()

        # Create a map of ER subjects by name
        er_subject_mapping = {subject.name: subject for subject in er_subjects}
        er_subject_names = set(er_subject_mapping.keys())

        # Identify buoys for insertion or update
        insert_buoys = set()
        update_buoys = set()

        observations = []

        for serial_number, buoy_states in buoy_states_by_serial_number.items():
            newest_buoy_state = buoy_states[0]
            # If the newest state has no location info and no changeRecords, skip
            if (
                (
                    newest_buoy_state.currentState.latDeg is None
                    or newest_buoy_state.currentState.lonDeg is None
                )
                and (
                    newest_buoy_state.currentState.recoveredLatDeg is None
                    or newest_buoy_state.currentState.recoveredLonDeg is None
                )
                and (len(newest_buoy_state.changeRecords) == 0)
            ):
                logger.warning(
                    f"Skipping buoy {serial_number} due to missing location data"
                )
                continue

            primary_subject_name = f"{self._prefix}{serial_number}_A"
            if primary_subject_name in er_subject_names:
                # Buoy exists in ER -> update
                update_buoys.add(serial_number)
            else:
                # Buoy does not exist in ER -> insert
                insert_buoys.add(serial_number)

        # Process inserts
        for serial_number in insert_buoys:
            buoy_records = buoy_states_by_serial_number[serial_number]
            for buoy_record in buoy_records:
                try:
                    new_observations = buoy_record.create_observations(self._prefix)
                    observations.extend(new_observations)
                except pydantic.ValidationError as ve:
                    logger.exception(
                        "Failed to create BuoyEvent for %s. Error: %s",
                        serial_number,
                        ve.json(),
                    )

        # Process updates
        for serial_number in update_buoys:
            buoy_records = buoy_states_by_serial_number[serial_number]
            primary_subject_name = f"{self._prefix}{serial_number}_A"
            er_subject = er_subject_mapping.get(primary_subject_name)
            if er_subject is None:
                logger.warning(
                    f"Primary ER subject not found for buoy {serial_number}. Skipping update."
                )
                continue

            for buoy_record in buoy_records:
                try:
                    # Use the last_position_date as the threshold
                    new_observations = buoy_record.create_observations(
                        self._prefix,
                        er_subject.last_position_date,
                    )
                    observations.extend(new_observations)
                except pydantic.ValidationError as ve:
                    logger.exception(
                        "Failed to update BuoyEvent for %s. Error: %s",
                        serial_number,
                        ve.json(),
                    )

        logger.info(f"Inserts: {insert_buoys}")
        logger.info(f"Updates: {update_buoys}")

        return observations