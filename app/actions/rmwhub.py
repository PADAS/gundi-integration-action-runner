import hashlib
from typing import Dict, List, Optional, Tuple

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Optional, Tuple

import httpx
import pytz
import stamina
from dateparser import parse as parse_date
from fastapi.encoders import jsonable_encoder
from gundi_core.schemas.v2.gundi import LogLevel
from pydantic import BaseModel, NoneStr, validator

from app.actions.buoy import BuoyClient
from app.services.activity_logger import log_action_activity

logger = logging.getLogger(__name__)


SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_device"
GEAR_DEPLOYED_EVENT = "gear_deployed"
GEAR_RETRIEVED_EVENT = "gear_retrieved"
EPOCH = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat()


class Status(Enum):
    DEPLOYED = "gear_deployed"
    RETRIEVED = "gear_retrieved"


class Trap(BaseModel):
    id: str
    sequence: int
    latitude: float
    longitude: float
    deploy_datetime_utc: Optional[NoneStr]
    surface_datetime_utc: Optional[NoneStr]
    retrieved_datetime_utc: Optional[NoneStr]
    status: str
    accuracy: str
    release_type: Optional[NoneStr]
    is_on_end: bool

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key):
        return self.__getitem__(key)

    def __hash__(self):
        return hash(
            (
                self.id,
                self.sequence,
                self.latitude,
                self.longitude,
                self.deploy_datetime_utc,
            )
        )

    def get_latest_update_time(self) -> datetime:
        """
        Get the last updated time of the trap based on the status.
        """

        if self.status == "deployed":
            return Trap.convert_to_utc(self.deploy_datetime_utc)
        elif self.status == "retrieved":
            return Trap.convert_to_utc(self.retrieved_datetime_utc)

    @classmethod
    # TODO: Convert to local function within get_latest_update_time after update status code is removed. RF-755
    def convert_to_utc(self, datetime_str: str) -> datetime:
        """
        Convert the datetime string to UTC.
        """
        naive_datetime_obj = parse_date(datetime_str)
        utc_datetime_obj = naive_datetime_obj.replace(tzinfo=timezone.utc)
        if not utc_datetime_obj:
            raise ValueError(f"Unable to parse datetime string: {datetime_str}")

        return utc_datetime_obj

    def shift_update_time(self):
        """
        Shift the update time of the trap by 5 seconds.
        """

        if self.status == "deployed":
            self.deploy_datetime_utc = (
                self.get_latest_update_time() + timedelta(seconds=5)
            ).isoformat()
        elif self.status == "retrieved":
            self.retrieved_datetime_utc = (
                self.get_latest_update_time() + timedelta(seconds=5)
            ).isoformat()


class GearSet(BaseModel):
    vessel_id: str
    id: str
    deployment_type: str
    traps_in_set: int
    trawl_path: str
    share_with: Optional[List[str]]
    traps: List[Trap]
    when_updated_utc: str

    @validator("trawl_path", pre=True)
    def none_to_empty(cls, v: object) -> object:
        if v is None:
            return ""
        return v

    @validator("share_with", pre=True)
    def none_to_empty_list(cls, v: object) -> object:
        if v is None:
            return []
        return v

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key):
        return self.__getitem__(key)

    def __hash__(self):
        return hash((self.id, self.deployment_type, tuple(self.traps)))

    def get_devices(self) -> List:
        """
        Get the devices info for the gear set.
        """

        devices = []
        for trap in self.traps:
            last_deployed = (
                trap.get_latest_update_time().isoformat()
                if trap.status == "deployed"
                else trap.deploy_datetime_utc
            )

            devices.append(
                {
                    "device_id": "rmwhub_"
                    + trap.id.removeprefix("e_")
                    .removeprefix("rmwhub_")
                    .removeprefix("device_")
                    .removeprefix("edgetech_"),
                    "label": "a" if trap.sequence == 1 else "b",
                    "location": {
                        "latitude": trap.latitude,
                        "longitude": trap.longitude,
                    },
                    "last_deployed": last_deployed,
                    "last_updated": last_deployed,
                }
            )

        return devices

    async def create_observations(self, er_subject: dict = None) -> List:
        """
        Create observations for the gear set.
        """

        devices = self.get_devices()

        observations = []

        for trap in self.traps:
            if (
                self.deployment_type == "trawl"
                and er_subject
                and RmwHubAdapter.clean_id_str(er_subject.get("name"))
                == RmwHubAdapter.clean_id_str(trap.id)
            ):
                if er_subject.get("additional") and (
                    er_subject_devices := er_subject.get("additional").get("devices")
                ):
                    if len(self.traps) != len(er_subject_devices):
                        trap.shift_update_time()

            if trap.status == "deployed":
                observations.append(
                    self.create_observation_for_event(trap, devices, Status.DEPLOYED)
                )
            elif trap.status == "retrieved":
                observations.append(
                    self.create_observation_for_event(trap, devices, Status.RETRIEVED)
                )
            else:
                logger.error(
                    f"Invalid status for trap ID {trap.id}. Status: {trap.status}"
                )
                return []

        logger.info(f"Created {len(observations)} observations for gear set {self.id}.")

        return observations

    def create_observation_for_event(
        self, trap: Trap, devices: List, event_status: Status
    ) -> dict:
        """
        Create an observation from the RMW Hub trap.
        """

        display_id_hash = hashlib.sha256(str(self.id).encode()).hexdigest()[:12]
        subject_name = "rmwhub_" + trap.id

        last_updated = trap.get_latest_update_time().isoformat()
        observation = {
            "name": subject_name,
            "source": subject_name,
            "type": SOURCE_TYPE,
            "subject_type": SUBJECT_SUBTYPE,
            "is_active": True if event_status == Status.DEPLOYED else False,
            "recorded_at": last_updated,
            "location": {"lat": trap.latitude, "lon": trap.longitude},
            "additional": {
                "subject_is_active": True if event_status == Status.DEPLOYED else False,
                "subject_name": subject_name,
                "rmwhub_set_id": self.id,
                "display_id": display_id_hash,
                "event_type": GEAR_DEPLOYED_EVENT
                if trap.status == "deployed"
                else GEAR_RETRIEVED_EVENT,
                "devices": devices,
            },
        }

        logger.info(
            f"Created observation for trap ID: {trap.id} with Subject name: {subject_name} with event type {event_status}."
        )

        return observation

    async def get_trap_ids(self) -> set:
        """
        Get the trap IDs for the gear set.
        """

        return {
            geartrap.id.replace("e_", "").replace("rmwhub_", "")
            for geartrap in self.traps
        }

    async def is_visited(self, visited: set) -> bool:
        """
        Check if the gearset has been visited.
        """

        traps_in_gearset = await self.get_trap_ids()
        return traps_in_gearset & visited


class RmwHubAdapter:
    def __init__(
        self,
        integration_id: str,
        api_key: str,
        rmw_url: str,
        er_token: str,
        er_destination: str,
        *args,
        **kwargs,
    ):
        self.integration_id = integration_id
        self.rmw_client = RmwHubClient(api_key, rmw_url)
        self.er_client = BuoyClient(er_token, er_destination)
        self.er_subject_name_to_subject_mapping = {}
        self.options = kwargs.get("options", {})

    async def download_data(
        self, start_datetime: str, status: bool = None
    ) -> List[GearSet]:
        """
        Downloads data from the RMW Hub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        response = await self.rmw_client.search_hub(start_datetime, status)
        response_json = json.loads(response)

        if "sets" not in response_json:
            logger.error(f"Failed to download data from RMW Hub API. Error: {response}")
            return []

        return self.convert_to_sets(response)

    async def _get_newest_set_from_rmwhub(self, devices):
        """
        Downloads data from the RMW Hub API using the search_own endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        if(not devices):
            return None
        target_traps = sorted([self.validate_id_length("e_" + self.clean_id_str(device['device_id'])) for device in devices])
        sets = await self.search_own(trap_id = target_traps[0])

        newest = None
        newestDate = None
        for gearset in sets:
            set_traps = sorted([trap.id for trap in gearset.traps])
            if(set_traps == target_traps):
                datecomp = parse_date(gearset.when_updated_utc)
                if(not newestDate or (datecomp > newestDate)):
                    newest = gearset
                    newestDate = datecomp
                    
        return newest

    async def search_own(self, trap_id = None) -> dict:
        """
        Downloads data from the RMWHub API using the search_own endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        url = self.rmw_url + "/search_own/"

        data = {"format_version": 0.1, "api_key": self.api_key}
        if(trap_id):
            data['trap_id'] = trap_id

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return self.convert_to_sets(response)

    def convert_to_sets(self, response: dict) -> List[GearSet]:
        response_json = json.loads(response)

        if "sets" not in response_json:
            logger.error(f"Failed to download data from RMW Hub API. Error: {response}")
            return []

        sets = response_json["sets"]
        gearsets = []
        for set in sets:
            traps = []
            for trap in set["traps"]:
                trap_obj = Trap(
                    id=trap["trap_id"],
                    sequence=trap["sequence"],
                    latitude=trap["latitude"],
                    longitude=trap["longitude"],
                    deploy_datetime_utc=trap["deploy_datetime_utc"],
                    surface_datetime_utc=trap["surface_datetime_utc"],
                    retrieved_datetime_utc=trap["retrieved_datetime_utc"],
                    status=trap["status"],
                    accuracy=trap["accuracy"],
                    release_type=trap["release_type"],
                    is_on_end=trap["is_on_end"],
                )
                traps.append(trap_obj)

            gearset = GearSet(
                vessel_id=set["vessel_id"],
                id=set["set_id"],
                deployment_type=set["deployment_type"],
                traps_in_set=set["traps_in_set"],
                trawl_path=set["trawl_path"],
                share_with=set.get("share_with", []),
                when_updated_utc=set["when_updated_utc"],
                traps=traps,
            )

            gearsets.append(gearset)

        return gearsets

    async def process_download(
        self, rmw_sets: List[GearSet], start_datetime: datetime, minute_interval: int
    ) -> List:
        """
        Process the sets from the RMW Hub API.
        """

        # Normalize the extracted data into a list of observations following to the Gundi schema:
        observations = []

        # Download Data from ER for the same time interval as RmwHub
        # TODO: Only download recent updates from ER
        er_subjects = await self.er_client.get_er_subjects()

        # Create maps of er_subject_names and rmw_trap_ids/set_ids
        # RMW trap IDs would be in the subject name
        self.er_subject_name_to_subject_mapping = (
            await self.create_name_to_subject_mapping(er_subjects)
        )
        self.er_subject_id_to_subject_mapping = dict(
            (subject.get("id"), subject) for subject in er_subjects
        )
        er_subject_names = set(self.er_subject_name_to_subject_mapping.keys())
        er_subject_ids = set(self.er_subject_id_to_subject_mapping.keys())
        er_subject_names_and_ids = er_subject_names | er_subject_ids

        # Iterate through rmwSets and determine what is an insert and what is an update to Earthranger
        rmw_inserts = set()
        rmw_updates = set()
        for gearset in rmw_sets:
            for trap in gearset.traps:
                if RmwHubAdapter.clean_id_str(trap.id) in er_subject_names_and_ids:
                    rmw_updates.add(gearset)
                else:
                    rmw_inserts.add(gearset)

        # Handle inserts to Earthranger
        visited_inserts = set()
        for gearset in rmw_inserts:
            logger.info(f"Rmw Set ID {gearset.id} not found in ER subjects. Inserting.")

            # Process each trap individually
            for trap in gearset.traps:
                if await gearset.is_visited(visited_inserts):
                    logger.info(
                        f"Skipping insert for trap ID {trap.id}. Already processed."
                    )
                    continue

                visited_inserts.update(await gearset.get_trap_ids())

                logger.info(f"Processing trap ID {trap.id} for insert to ER.")

                # Create observations for the gear set from RmwHub
                new_observations = await self._create_observations(gearset)

                observations.extend(new_observations)
                logger.info(
                    f"Processed {len(new_observations)} new observations for trap ID {trap.id}."
                )

                if len(new_observations) == 0:
                    # New observations dict will be empty if ER has the latest update
                    logger.info(f"ER has the most recent update for trap ID {trap.id}.")

        # Handle updates to Earthranger
        visited_updates = set()
        for gearset in rmw_updates:
            logger.info(f"Rmw Set ID {gearset.id} found in ER subjects. Updating.")

            for trap in gearset.traps:
                # Process each trap individually
                if await gearset.is_visited(visited_updates):
                    logger.info(
                        f"Skipping insert for trap ID {trap.id}. Already processed."
                    )
                    continue

                visited_updates.update(await gearset.get_trap_ids())
                logger.info(f"Processing trap ID {trap.id} for update to ER.")

                # Get subject from ER
                clean_trap_id = RmwHubAdapter.clean_id_str(trap.id)
                if clean_trap_id in self.er_subject_name_to_subject_mapping.keys():
                    er_subject = self.er_subject_name_to_subject_mapping.get(
                        clean_trap_id
                    )
                elif clean_trap_id in self.er_subject_id_to_subject_mapping.keys():
                    er_subject = self.er_subject_id_to_subject_mapping.get(
                        clean_trap_id
                    )
                else:
                    logger.error(
                        f"Subject ID {clean_trap_id} not found in ER subjects."
                    )
                    er_subject = None

                # Create observations for the gear set from RmwHub
                new_observations = await self._create_observations(gearset, er_subject)

                observations.extend(new_observations)
                logger.info(
                    f"Processed {len(new_observations)} new observations for trap ID {trap.id}."
                )

                if len(new_observations) == 0:
                    # New observations dict will be empty if ER has the latest update
                    logger.info(f"ER has the most recent update for trap ID {trap.id}.")

        return observations

    def _create_traps_gearsets_mapping_key(self, traps_ids: List[str]) -> str:
        """
        Create a unique key for the traps and gearsets mapping.
        """
        sorted_traps_ids = sorted(traps_ids)
        cleaned_traps_ids = [
            RmwHubAdapter.clean_id_str(trap_id) for trap_id in sorted_traps_ids
        ]
        "".join(cleaned_traps_ids)
        return "".join(cleaned_traps_ids)

    async def generate_display_id_from_devices(self, devices):
        concat_devices = self._create_traps_gearsets_mapping_key(
            [device.get("device_id") for device in devices]
        )
        display_id_hash = hashlib.sha256(str(concat_devices).encode()).hexdigest()[:12]

        return display_id_hash

    # Trap IDs must be atleast 32 characters and no more than 38 characters
    def validate_id_length(self, id_str: str):
        return id_str.ljust(32, "#")

    async def process_upload(self, start_datetime: datetime) -> Tuple[List, dict]:
        """
        Process the sets from the Buoy API and upload to RMWHub.
        Returns a list of new observations for Earthranger with the new RmwHub set IDs.
        """

        logger.info("Processing updates to RMW Hub from ER...")

        # Normalize the extracted data into a list of updates following to the RMWHub schema:
        updates = []

        # Get updates from the last interval_minutes in ER
        er_subjects = await self.er_client.get_er_subjects(start_datetime)
        if not er_subjects:
            await log_action_activity(
                integration_id=self.integration_id,
                action_id="pull_observations",
                title="No subjects with new observations found in ER.",
                level=LogLevel.INFO,
            )
            return 0, {}

        # Iterate through er_subjects and determine what is an insert and what is an update to RmwHub
        # Based on the display ID existence on the RMW side
        for subject in er_subjects:
            subject_name = subject.get("name")

            if subject_name.startswith("rmw"):
                logger.debug(f"Subject ID {subject_name} originally from rmwHub. Skipped.")
                continue
            elif not subject_name:
                logger.error(f"Subject ID {subject['id']} has no name. No action.")
                continue

            latest_observation = await self.er_client.get_latest_observations(subject['id'], 1)
            if(not latest_observation):
                logging.info(f"No latest observation found for subject {subject['id']}.  Skipping...")
                continue
            latest_observation = latest_observation[0]

            devices = latest_observation.get('observation_details', {}).get('devices', [])
            if(not devices):
                logging.info(f"No devices in latest observation for subject {subject['id']}.  Skipping...")
                continue
            
            rmwhub_set = await self._get_newest_set_from_rmwhub(devices)

            if(rmwhub_set and (parse_date(rmwhub_set.when_updated_utc) > parse_date(latest_observation['created_at']))):
                continue

            new_gearset = await self._create_rmw_update_from_er_subject(subject, latest_observation, rmwhub_set)
            if(new_gearset):
                updates.append(new_gearset)

        if not updates:
            logger.info("No updates to upload to RMW Hub API.")
            return 0, {"trap_count": 0}
    
        response = await self._upload_data(updates)
        num_new_observations = len(
            [trap.id for gearset in updates for trap in gearset.traps]
        )
        return num_new_observations, response

    # TODO RF-752: Remove unecessary code when status updates are verified to be working through event
    # type in API instead of is_active status on observation.
    async def push_status_updates(self, observations: List, rmw_sets: List[GearSet]):
        """
        Process the status updates from the RMW Hub API.
        """

        rmw_set_id_to_gearset_mapping = await self.create_set_id_to_gearset_mapping(rmw_sets)

        visited_traps = set()
        for observation in observations:
            rmw_set_id = RmwHubAdapter.clean_id_str(
                observation.get("additional").get("rmwhub_set_id")
            )

            if rmw_set_id not in rmw_set_id_to_gearset_mapping.keys():
                await log_action_activity(
                    integration_id=self.integration_id,
                    action_id="pull_observations",
                    title=f"RMW Set ID {rmw_set_id} not found in RMW sets. No action.",
                    level=LogLevel.ERROR,
                )
                continue

            rmw_set = rmw_set_id_to_gearset_mapping[rmw_set_id]

            for trap in rmw_set.traps:
                if trap.id in visited_traps:
                    logger.info(
                        f"Skipping update for trap ID {trap.id}. Already processed."
                    )
                    continue

                visited_traps.add(trap.id)

                # Get subject from ER
                clean_trap_id = RmwHubAdapter.clean_id_str(trap.id)
                if clean_trap_id in self.er_subject_name_to_subject_mapping.keys():
                    er_subject = self.er_subject_name_to_subject_mapping.get(
                        clean_trap_id
                    )
                elif clean_trap_id in self.er_subject_id_to_subject_mapping.keys():
                    er_subject = self.er_subject_id_to_subject_mapping.get(
                        clean_trap_id
                    )
                else:
                    er_subject = None
                    logger.info(f"Subject ID {clean_trap_id} not found in ER subjects.")

                if er_subject:
                    await self._update_status(trap, er_subject)
                else:
                    await self._update_status(trap)

    # TODO RF-752: Remove unecessary code when status updates are verified to be working through event
    # type in API instead of is_active status on observation.
    async def _update_status(self, trap: Trap, er_subject: dict = None):
        """
        Update the status of the ER subject based on the RMW status and deployment/retrieval times
        """

        # Determine if ER or RMW has the most recent update in order to update status in ER:
        if er_subject:
            er_last_updated = datetime.fromisoformat(er_subject.get("updated_at"))

        trap_latest_update_time = trap.get_latest_update_time()

        if er_subject and er_last_updated > trap_latest_update_time:
            return
        elif er_subject and (er_last_updated < trap_latest_update_time):
            await self.er_client.patch_er_subject_status(
                er_subject.get("name"), True if trap.status == "deployed" else False
            )
        elif not er_subject:
            logger.error(
                f"Insert operation for Trap {trap.id}. Cannot update subject that does not exist."
            )

            trap_id_in_er = "rmwhub_" + (RmwHubAdapter.clean_id_str(trap.id))
            async for attempt in stamina.retry_context(
                on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0, wait_max=32.0
            ):
                with attempt:
                    await self.er_client.patch_er_subject_status(
                        trap_id_in_er, True if trap.status == "deployed" else False
                    )
        else:
            logger.error(
                f"Failed to compare gear set for trap ID {trap.id}. RMW latest update time: {trap_latest_update_time.isoformat()}"
            )

    async def _create_observations(
        self, rmw_set: GearSet, er_subject: dict = None
    ) -> List:
        """
        Create new observations for ER from RmwHub data.

        Returns an empty list if ER has the most recent updates. Otherwise, list of new observations to write to ER.
        """

        # Create observations for the gear set
        observations = await rmw_set.create_observations(er_subject)

        return observations

    async def _create_put_er_set_id_observation(
        self, er_subject: dict, set_id: str
    ) -> int:
        """
        Update the set ID for the ER subject based on the provided set ID.
        Returns 1 if the observation was created for the ER subject, 0 otherwise.
        """
        devices = er_subject.get("additional").get("devices", [])
        display_id_hash = self.generate_display_id_from_devices(devices)

        is_active = er_subject.get("is_active")
        source_provider = await self.er_client.get_source_provider(er_subject.get("id"))

        observations = []
        for device in devices:
            observations.append(
                {
                    "name": device["device_id"],
                    "source": device["device_id"],
                    "manufacturer_id": device["device_id"],
                    "type": SOURCE_TYPE,
                    "subject_type": SUBJECT_SUBTYPE,
                    "is_active": is_active,
                    # TODO: SPIKE to determine if this should be device["last_updated"]
                    "recorded_at": datetime.now().isoformat(),
                    "location": {
                        "lat": device["location"]["latitude"],
                        "lon": device["location"]["longitude"],
                    },
                    "additional": {
                        "subject_is_active": is_active,
                        "subject_name": device["device_id"],
                        "rmwhub_set_id": set_id,
                        "display_id": display_id_hash,
                        "event_type": GEAR_DEPLOYED_EVENT
                        if is_active
                        else GEAR_RETRIEVED_EVENT,
                        "devices": er_subject["additional"]["devices"],
                    },
                }
            )

        # Send observations to Gundi v1 Sensors API
        created = 0
        for observation in observations:
            created += await self.er_client.create_v1_observation(
                source_provider, observation
            )

        return created

    async def _upload_data(
        self,
        updates: List[GearSet],
    ) -> dict:
        """
        Upload data to the RMWHub API using the RMWHubClient.

        Return RMWHub response if upload is successful, empty dict otherwise
        """

        response = await self.rmw_client.upload_data(updates)

        if response.status_code == 200:
            logger.info("Upload to RMW Hub API was successful.")
            result = json.loads(response.content)
            if len(result["result"]):
                logger.info(
                    f"Number of traps uploaded: {result['result']['trap_count']}"
                )
                logger.info(
                    f"Number of failed sets: {len(result['result']['failed_sets'])}"
                )
                return result["result"]

            logger.error(f"No info returned from RMW Hub API.")
            return {}
        else:
            logger.error(
                f"Failed to upload data to RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return {}

    async def _create_rmw_update_from_er_subject(
        self,
        er_subject: dict,
        latest_observation: dict = None,
        rmw_gearset: GearSet = None,
    ) -> Optional[GearSet]:
        """
        Create new updates from ER data for upload to RMWHub.

        :param er_subject: ER subject to create updates from
        :param rmw_gearset: RMW gear set to update (not required for new inserts)
        :param latest_observation: Latest observation for the subject (not required for new inserts)
        """

        # Create traps list:
        traps = []
        if not er_subject.get("additional") or not er_subject.get("additional").get(
            "devices"
        ):
            logger.error(f"No traps found for trap ID {er_subject.get('name')}.")
            return None

        deployed = er_subject.get("is_active")
        additional_data = er_subject.get("additional", {})

        trap_id_mapping = (
            {RmwHubAdapter.clean_id_str(trap.id): trap for trap in rmw_gearset.traps}
            if rmw_gearset
            else {}
        )

        devices = (
            latest_observation.get("observation_details", {}).get("devices")
            if latest_observation
            else additional_data.get("devices")
        )
        latest_observation_datetime = (
            latest_observation.get("recorded_at") if latest_observation else None
        )
        for device in devices:
            # Use just the ID for the Trap ID if the gearset is originally from RMW
            subject_name = er_subject.get("name")
            device_name = device.get("device_id")
            cleaned_id = RmwHubAdapter.clean_id_str(device_name)
            trap_id = (
                cleaned_id
                if rmw_gearset and subject_name.startswith("rmw")
                else "e_" + cleaned_id
            )

            if not deployed and not rmw_gearset:
                msg = f"This trap ({trap_id}) is not being deployed and still does not exist in RMW Hub, skipping."
                log_action_activity(
                    integration_id=self.integration_id,
                    action_id="pull_observations",
                    title=msg,
                    level=LogLevel.WARNING,
                )
                logger.warning(msg)
                continue

            rmw_trap_datetime = (
                latest_observation_datetime
                if latest_observation
                else device.get("last_updated")
            )
            rmw_trap_datetime = (
                self.convert_datetime_to_utc(rmw_trap_datetime)
                if rmw_trap_datetime
                else None
            )
            # deploy_datetime_utc is required, so in retrieve events, we will use the current deployed datetime
            current_deployed_datetime = (
                trap_id_mapping.get(
                    RmwHubAdapter.clean_id_str(trap_id)
                ).deploy_datetime_utc
                if not deployed
                else None
            )
            traps.append(
                Trap(
                    id=self.validate_id_length(trap_id),
                    sequence=1 if device.get("label") == "a" else 2,
                    latitude=device.get("location").get("latitude"),
                    longitude=device.get("location").get("longitude"),
                    deploy_datetime_utc=rmw_trap_datetime
                    if deployed
                    else current_deployed_datetime,
                    surface_datetime_utc=rmw_trap_datetime if deployed else None,
                    retrieved_datetime_utc=None if deployed else rmw_trap_datetime,
                    status="deployed" if deployed else "retrieved",
                    accuracy="",
                    is_on_end=True,
                )
            )

        # No traps found for the gear set it will be skipped
        if len(traps) == 0:
            return None

        # Create gear set:
        if not rmw_gearset:
            set_id = "e_" + str(uuid.uuid4())
            vessel_id = ""
        else:
            set_id = rmw_gearset.id
            vessel_id = rmw_gearset.vessel_id

        share_with = self.options.get("share_with", [])
        gear_set = GearSet(
            vessel_id=vessel_id,
            id=set_id,
            deployment_type="trawl" if len(traps) > 1 else "single",
            traps_in_set=len(traps),
            trawl_path="",
            share_with=share_with,
            traps=traps,
            when_updated_utc=datetime.now(timezone.utc).isoformat(),
        )

        return gear_set

    async def create_set_id_to_gearset_mapping(self, sets: List[GearSet]) -> dict:
        """
        Create a mapping of Set IDs to GearSets.
        """

        set_id_to_set_mapping = {}
        for gear_set in sets:
            set_id_to_set_mapping[RmwHubAdapter.clean_id_str(gear_set.id)] = gear_set
        return set_id_to_set_mapping

    async def create_name_to_subject_mapping(self, er_subjects: List) -> dict:
        """
        Create a mapping of ER subject names to subjects.
        """

        name_to_subject_mapping = {}
        for subject in er_subjects:
            if subject.get("name"):
                name_to_subject_mapping[
                    RmwHubAdapter.clean_id_str(subject.get("name"))
                ] = subject
            else:
                msg = "Cannot clean string. Subject name is empty."
                await log_action_activity(
                    integration_id=self.integration_id,
                    action_id="pull_observations",
                    title=msg,
                    level=LogLevel.ERROR,
                )
        return name_to_subject_mapping

    @classmethod
    def clean_id_str(cls, subject_name: str):
        """
        Resolve the ID string to just the UUID
        """
        if not subject_name:
            msg = "Cannot clean string. Subject name is empty."
            logger.error(msg)
            return None

        cleaned_str = (
            subject_name.removeprefix("device_")
            .removeprefix("rmwhub_")
            .removeprefix("rmw_")
            .removeprefix("e_")
            .removeprefix("edgetech_")
            .rstrip("#")
            .lower()
        )
        return cleaned_str

    def convert_datetime_to_utc(self, datetime_str: str) -> str:
        """
        Convert the datetime string to UTC format.
        """
        if datetime_str.endswith("Z"):
            datetime_str = datetime_str[:-1] + "+00:00"
        datetime_obj = datetime.fromisoformat(datetime_str)
        datetime_obj = datetime_obj.astimezone(pytz.utc)
        formatted_datetime = datetime_obj.isoformat()

        return formatted_datetime


class RmwHubClient:
    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, api_key: str, rmw_url: str):
        self.api_key = api_key
        self.rmw_url = rmw_url

    async def search_hub(self, start_datetime: str, status: bool = None) -> dict:
        """
        Downloads data from the RMWHub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "max_sets": 2000,
            # "status": "deployed", // Pull all data not just deployed gear
            "start_datetime_utc": start_datetime.astimezone(pytz.utc).isoformat()
        }

        if status:
            data["status"] = status

        url = self.rmw_url + "/search_hub/"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text

    async def upload_data(self, updates: List[GearSet]) -> httpx.Response:
        """
        Upload data to the RMWHub API using the upload_data endpoint.
        ref: https://ropeless.network/api/docs
        """

        url = self.rmw_url + "/upload_deployments/"
        sets = [jsonable_encoder(update) for update in updates]

        for set_entry in sets:
            set_entry["set_id"] = set_entry.pop("id")
            for trap in set_entry["traps"]:
                trap["trap_id"] = trap.pop("id")
                trap["release_type"] = trap.get("release_type") or ""

        upload_data = {"format_version": 0, "api_key": self.api_key, "sets": sets}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, headers=RmwHubClient.HEADERS, json=upload_data
            )

        if response.status_code != 200:
            logger.error(
                f"Failed to upload data to RMW Hub API. Error: {response.status_code} - {response.content}"
            )

        return response
