import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pydantic

from app.actions.buoy import BuoyClient, ObservationSubject
from app.actions.edgetech.types import Buoy

logger = logging.getLogger(__name__)


class EdgeTechProcessor:
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

        By default, this method defines a time window starting 30 minutes before the current UTC time.

        Returns:
            Dict[str, Any]: A dictionary with 'start_datetime' keys defining the filter window.
        """
        start_datetime = datetime.now(timezone.utc) - timedelta(minutes=30)
        return {"start_datetime": start_datetime}

    def _should_skip_buoy(self, record: Buoy) -> Tuple[bool, Optional[str]]:
        """
        Determine if a buoy record should be skipped and why.

        Args:
            record (Buoy): The buoy record to check.

        Returns:
            Tuple[bool, Optional[str]]: A tuple containing:
                - bool: True if the record should be skipped, False otherwise
                - Optional[str]: The reason for skipping, or None if not skipped
        """
        if record.currentState.isDeleted:
            return (
                True,
                f"Skipping deleted buoy record with serial number {record.serialNumber}. Last updated at {record.currentState.lastUpdated}.",
            )

        if not record.currentState.isDeployed:
            return (
                True,
                f"Skipping buoy record with serial number {record.serialNumber} that is not deployed. Last updated at {record.currentState.lastUpdated}.",
            )

        if not record.has_location:
            return (
                True,
                f"Skipping buoy record with serial number {record.serialNumber} that has no location data. Last updated at {record.currentState.lastUpdated}.",
            )

        return False, None

    def _filter_edgetech_buoys_data(self, data: List[Buoy]) -> List[Buoy]:
        """
        Filter buoy data records based on their current state.

        Iterates over the parsed buoy data and selects records whose current state is not deleted
        and is deployed. If a record is deleted or not deployed, it is skipped.

        Returns:
            List[Buoy]: A list of Buoy objects that satisfy the filter criteria.
        """
        filtered_data: List[Buoy] = []
        skipped_serial_numbers: Set[str] = set()

        for record in data:
            should_skip, skip_reason = self._should_skip_buoy(record)

            if should_skip:
                logger.warning(skip_reason)
                skipped_serial_numbers.add(record.serialNumber)
                continue

            filtered_data.append(record)

        return filtered_data

    def _get_latest_buoy_states(self, data: List[Buoy]) -> List[Buoy]:
        """
        Retrieve the latest buoy states from the parsed data.

        Returns:
            List[Buoy]: A list of Buoy objects representing the latest states.
        """
        latest: Dict[str, Buoy] = {}

        for record in data:
            key = record.serialNumber
            prev = latest.get(key)
            if (
                prev is None
                or record.currentState.lastUpdated > prev.currentState.lastUpdated
            ):
                latest[key] = record

        return list(latest.values())

    async def _identify_buoys(
        self,
        er_subjects_name_to_subject_mapping: Dict[str, ObservationSubject],
        serial_number_to_edgetech_buoy: Dict[str, Buoy],
    ) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Determines which buoys need to be inserted (deployed) or updated (deployed or hauled)

        This method checks the existing ER subjects against the latest buoy states and categorizes them into:
        - `to_deploy`: Buoys that need to be inserted into the ER system.
        - `to_haul`: Buoys that need to be updated in the ER system.
        - `to_update`: Buoys that are already in the ER system and need to be updated.

        Args:
            er_subjects_name_to_subject_mapping (Dict[str, Any]): A mapping of ER subject names to their corresponding subjects.
            serial_number_to_edgetech_buoy (Dict[str, Any]): A mapping of serial numbers to Edgetech buoy objects.
        Returns:
            Tuple[Set[str], Set[str], Set[str]]: A tuple containing two sets:
                - `to_deploy`: Serial numbers of buoys that need to be set as deployed.
                - `to_haul`: Serial numbers of buoys that need to be set as hauled.
                - `to_update`: Serial numbers of buoys that are already in the ER system and need to be updated.
        """
        to_deploy: Set[str] = set()
        to_haul: Set[str] = set()
        to_update: Set[str] = set()

        for serial_number in serial_number_to_edgetech_buoy.keys():
            primary_subject_name = f"{self._prefix}{serial_number}_A"

            if primary_subject_name not in er_subjects_name_to_subject_mapping:
                to_deploy.add(serial_number)
            else:
                # If the buoy is already in the ER system, we check if it needs to be updated.
                er_subject = er_subjects_name_to_subject_mapping[primary_subject_name]
                edgetech_buoy = serial_number_to_edgetech_buoy[serial_number]

                # Check if the buoy's last updated time is more recent than the ER subject's last updated time.
                edgetech_buoy_last_updated = (
                    edgetech_buoy.currentState.lastUpdated.replace(microsecond=0)
                )
                er_subject_last_position_date = er_subject.last_position_date.replace(
                    microsecond=0
                )
                if edgetech_buoy_last_updated > er_subject_last_position_date:
                    to_update.add(serial_number)

        for subject_name in er_subjects_name_to_subject_mapping.keys():
            subject_name = subject_name.replace(self._prefix, "")
            subject_serial_number = subject_name.split("_")[0]
            # If the subject_serial_number is not one of the currently deployed buoys in the Edgetech data, it means it has been hauled.
            if subject_serial_number not in serial_number_to_edgetech_buoy:
                to_haul.add(subject_serial_number)

        logger.info(f"Buoys to deploy: {to_deploy}")
        logger.info(f"Buoys to haul: {to_haul}")
        logger.info(f"Buoys to update: {to_haul}")

        return to_deploy, to_haul, to_update

    async def process(self) -> List[Dict[str, Any]]:
        """
        Process buoy data to generate observation events for the ER system.

        This asynchronous method performs the following steps:
            1. Retrieves and filters the latest buoy states grouped by serial number.
            2. Fetches existing ER subjects from the ER client.
            3. Maps ER subjects by name and categorizes buoy states into:
                - Inserts: Buoys not yet present in ER.
                - Updates: Buoys present in ER that need to be updated.
                - No-ops: Buoys with no location data or no changes.
            4. Creates observation events for new or updated buoys.
            5. Logs information about inserts, updates, and no-ops.

        Returns:
            List[dict]: A list of dictionaries representing the observation events generated during processing.
        """
        edgetech_deployed_buoys = self._filter_edgetech_buoys_data(self._data)
        edgetech_deployed_buoys = self._get_latest_buoy_states(edgetech_deployed_buoys)
        er_subjects = await self._er_client.get_er_subjects(
            params={
                "include_details": "true",
                "position_updated_since": self._filters["start_datetime"].isoformat(),
            }
        )

        er_subject_name_to_subject_mapping = {
            subject.name: subject for subject in er_subjects
        }
        serial_number_to_edgetech_buoy = {
            buoy.serialNumber: buoy for buoy in edgetech_deployed_buoys
        }

        to_deploy, to_haul, to_update = await self._identify_buoys(
            er_subject_name_to_subject_mapping,
            serial_number_to_edgetech_buoy,
        )

        observations = []

        for serial_number in to_deploy:
            edgetech_buoy = serial_number_to_edgetech_buoy[serial_number]
            try:
                # Get end unit buoy if this is a two-unit line
                end_unit_buoy = None
                if edgetech_buoy.currentState.isTwoUnitLine:
                    end_unit_buoy = serial_number_to_edgetech_buoy.get(
                        edgetech_buoy.currentState.endUnit
                    )
                    if not end_unit_buoy:
                        logger.warning(
                            "End unit buoy %s not found for serial number %s, skipping deployment.",
                            edgetech_buoy.currentState.endUnit,
                            serial_number,
                        )
                        continue
                    if edgetech_buoy.currentState.startUnit:
                        # This record it's for the end unit, so we skip it since it will be handled by the start unit buoy
                        continue

                to_deploy_observations = edgetech_buoy.create_observations(
                    prefix=self._prefix,
                    is_deployed=True,
                    end_unit_buoy=end_unit_buoy,
                )
                observations.extend(to_deploy_observations)
            except pydantic.ValidationError as ve:
                logger.exception(
                    "Failed to create BuoyEvent for %s. Error: %s",
                    serial_number,
                    ve.json(),
                )

        for serial_number in to_update:
            edgetech_buoy = serial_number_to_edgetech_buoy[serial_number]
            edgetech_buoy_lat = edgetech_buoy.currentState.latDeg
            edgetech_buoy_long = edgetech_buoy.currentState.lonDeg

            primary_subject_name = f"{self._prefix}{serial_number}_A"
            er_subject = er_subject_name_to_subject_mapping[primary_subject_name]
            er_subject_lat, er_subject_long = er_subject.location

            if (
                er_subject_lat == edgetech_buoy_lat
                and er_subject_long == edgetech_buoy_long
            ):
                # No change in location, skip update
                logger.info(
                    "No change in location for buoy %s, skipping update.",
                    serial_number,
                )
                continue

            try:
                # Get end unit buoy if this is a two-unit line
                end_unit_buoy = None
                if edgetech_buoy.currentState.isTwoUnitLine:
                    if edgetech_buoy.currentState.endUnit:
                        end_unit_buoy = serial_number_to_edgetech_buoy.get(
                            edgetech_buoy.currentState.endUnit
                        )
                        if not end_unit_buoy:
                            logger.warning(
                                "End unit buoy %s not found for serial number %s, skipping deployment.",
                                edgetech_buoy.currentState.endUnit,
                                serial_number,
                            )
                            continue
                    if edgetech_buoy.currentState.startUnit:
                        # This record it's for the end unit, so we skip it since it will be handled by the start unit buoy
                        continue

                to_update_observations = edgetech_buoy.create_observations(
                    prefix=self._prefix,
                    is_deployed=True,
                    end_unit_buoy=end_unit_buoy,
                )
                observations.extend(to_update_observations)
            except Exception as e:
                logger.exception(
                    "Failed to create BuoyEvent for %s. Error: %s",
                    serial_number,
                    e.json(),
                )

        for serial_number in to_haul:
            primary_subject_name = f"{self._prefix}{serial_number}_A"

            if primary_subject_name not in er_subject_name_to_subject_mapping:
                logger.warning(
                    "No ER subject found for serial number %s, skipping haul.",
                    serial_number,
                )
                continue

            er_subject = er_subject_name_to_subject_mapping[primary_subject_name]
            try:
                to_haul_observation = er_subject.create_observation(
                    recorded_at=datetime.now(timezone.utc),
                )
                observations.append(to_haul_observation)
            except pydantic.ValidationError as ve:
                logger.exception(
                    "Failed to create haul observation for %s. Error: %s",
                    serial_number,
                    ve.json(),
                )

        return observations
