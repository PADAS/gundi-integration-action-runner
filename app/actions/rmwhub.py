from datetime import datetime, timedelta
from enum import Enum
import hashlib
from typing import List

import json
import logging
import httpx
import pytz
from pydantic import validator, BaseModel
import requests
import stamina

from app.actions.buoy import BuoyClient


logger = logging.getLogger(__name__)


SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_device"
GEAR_DEPLOYED_EVENT = "gear_deployed"
GEAR_RETRIEVED_EVENT = "gear_retrieved"


class Status(Enum):
    DEPLOYED = "gear_deployed"
    RETRIEVED = "gear_retrieved"


class Trap(BaseModel):
    id: str
    sequence: int
    latitude: float
    longitude: float
    deploy_datetime_utc: str
    surface_datetime_utc: str
    retrieved_datetime_utc: str
    status: str
    accuracy: str
    release_type: str
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

        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
        datetime_obj = datetime_obj.astimezone(pytz.utc)
        return datetime_obj


class GearSet(BaseModel):
    vessel_id: str
    id: str
    deployment_type: str
    traps_in_set: int
    trawl_path: str
    share_with: List[str]
    traps: List[Trap]

    @validator("trawl_path", pre=True)
    def none_to_empty(cls, v: object) -> object:
        if v is None:
            return ""
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
            devices.append(
                {
                    "device_id": "rmwhub_" + str(RmwHubAdapter.clean_id_str(trap.id)),
                    "label": "a" if trap.sequence == 1 else "b",
                    "location": {
                        "latitude": trap.latitude,
                        "longitude": trap.longitude,
                    },
                    "last_updated": trap.get_latest_update_time().isoformat(),
                }
            )

        return devices

    def create_observations(self) -> List:
        """
        Create observations for the gear set.
        """

        devices = self.get_devices()

        observations = []

        for trap in self.traps:
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

    def get_trap_ids(self) -> set:
        """
        Get the trap IDs for the gear set.
        """

        return {RmwHubAdapter.clean_id_str(geartrap.id) for geartrap in self.traps}

    def is_visited(self, visited: set) -> bool:
        """
        Check if the gearset has been visited.
        """

        traps_in_gearset = self.get_trap_ids()
        return traps_in_gearset & visited


class RmwSets(BaseModel):
    sets: List[GearSet]


class RmwHubAdapter:
    def __init__(self, api_key: str, rmw_url: str, er_token: str, er_destination: str):
        self.rmw_client = RmwHubClient(api_key, rmw_url)
        self.er_client = BuoyClient(er_token, er_destination)
        self.er_subject_name_to_subject_mapping = {}

    async def download_data(
        self, start_datetime_str: str, minute_interval: int, status: bool = None
    ) -> RmwSets:
        """
        Downloads data from the RMW Hub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        response = await self.rmw_client.search_hub(
            start_datetime_str, minute_interval, status
        )
        response_json = json.loads(response)

        if "sets" not in response_json:
            logger.error(f"Failed to download data from RMW Hub API. Error: {response}")
            return RmwSets(sets=[])

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
                share_with=set["share_with"],
                traps=traps,
            )

            gearsets.append(gearset)

        return RmwSets(sets=gearsets)

    async def process_rmw_download(
        self, rmw_sets: RmwSets, start_datetime_str: str, minute_interval: int
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
        self.er_subject_name_to_subject_mapping = dict(
            (RmwHubAdapter.clean_id_str(subject.get("name")), subject)
            for subject in er_subjects
        )
        self.er_subject_id_to_subject_mapping = dict(
            (subject.get("id"), subject) for subject in er_subjects
        )
        er_subject_names_and_ids = set(
            self.er_subject_name_to_subject_mapping.keys()
        ).union(self.er_subject_id_to_subject_mapping.keys())

        # Iterate through rmwSets and determine what is an insert and what is an update to Earthranger
        rmw_inserts = set()
        rmw_updates = set()
        for gearset in rmw_sets.sets:
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

                if gearset.is_visited(visited_inserts):
                    logger.info(
                        f"Skipping insert for trap ID {trap.id}. Already processed."
                    )
                    continue

                visited_inserts.update(gearset.get_trap_ids())

                logger.info(f"Processing trap ID {trap.id} for insert to ER.")

                # Create observations for the gear set from RmwHub
                new_observations = await self.create_observations(gearset)

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
                if gearset.is_visited(visited_updates):
                    logger.info(
                        f"Skipping insert for trap ID {trap.id}. Already processed."
                    )
                    continue

                visited_updates.update(gearset.get_trap_ids())
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
                new_observations = await self.create_observations(gearset)

                observations.extend(new_observations)
                logger.info(
                    f"Processed {len(new_observations)} new observations for trap ID {trap.id}."
                )

                if len(new_observations) == 0:
                    # New observations dict will be empty if ER has the latest update
                    logger.info(f"ER has the most recent update for trap ID {trap.id}.")

        return observations

    # TODO: Work in progress
    async def process_er_updates(self, rmw_sets: RmwSets, start_datetime_str) -> List:
        """
        Process the sets from the Buoy API and upload to RMWHub.
        """

        # Normalize the extracted data into a list of updates following to the RMWHub schema:
        updates = []

        # Handle updates from Earthranger to RMW
        # Get updates from the last interval_minutes in ER
        er_subjects = self.er_client.get_er_subjects(start_datetime_str)

        # Get rmw trap IDs
        rmw_trap_ids = set()
        for gearset in rmw_sets.sets:
            for trap in gearset.traps:
                rmw_trap_ids.add(trap.id)

        # Iterate through er_subjects and determine what is an insert and what is an update to Earthranger
        er_inserts = set()
        er_updates = set()
        for subject in er_subjects:
            if (
                RmwHubAdapter.clean_id_str(subject.get("name"))
                .replace("e_", "")
                .replace("rmw_", "")
                in rmw_trap_ids
            ):
                er_updates.add(subject)
            else:
                er_inserts.add(subject)

        # Handle inserts to RMW
        for subject in er_inserts:
            logger.info(f"Subject ID {subject.get('name')} not found in RMW traps.")

            # Create update for the gear set for RmwHub

            # Update ER subject with set ID

        # Process upload data to RmwHub
        # await self.upload_data(
        #     upload_to_rmw,
        #     er_subject_name_to_subject_mapping,
        #     subject_id_to_subjectsource_mapping,
        # )

    async def push_status_updates(self, observations: List, rmw_sets: RmwSets):
        """
        Process the status updates from the RMW Hub API.
        """

        rmw_set_id_to_gearset_mapping = self.create_set_id_to_gearset_mapping(
            rmw_sets.sets
        )

        visited_traps = set()
        for observation in observations:
            rmw_set_id = RmwHubAdapter.clean_id_str(
                observation.get("additional").get("rmwhub_set_id")
            )
            rmw_set = rmw_set_id_to_gearset_mapping[rmw_set_id]

            for trap in rmw_set.traps:
                if trap.id in visited_traps:
                    logger.info(
                        f"Skipping update for trap ID {trap.id}. Already processed."
                    )
                    continue

                visited_traps.add(trap.id)

                # Get subject from ER
                clean_trap_id = trap.id.replace("e_", "").replace("rmwhub_", "")
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
                    await self.update_status(trap, er_subject)
                else:
                    await self.update_status(trap)

    # TODO: - Work in progress: Execute workaround for updating ER status using stamina
    async def update_status(self, trap: Trap, er_subject: dict = None):
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
            trap_id_in_er = "rmwhub_" + RmwHubAdapter.clean_id_str(trap.id)
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

    async def create_observations(self, rmw_set: GearSet) -> List:
        """
        Create new observations for ER from RmwHub data.

        Returns an empty list if ER has the most recent updates. Otherwise, list of new observations to write to ER.
        """

        # Create observations for the gear set
        observations = rmw_set.create_observations()

        return observations

    async def upload_data(
        self,
        upload_to_rmw: set,
        er_subject_name_to_subject_mapping: dict,
        er_subject_id_to_subjectsource_mapping: dict,
    ):
        """
        Upload data to the RMWHub API using the RMWHubClient.
        """

        new_rmw_observations = []
        for er_subject_id in upload_to_rmw:
            # Add new observation for current Trap ID in ER
            new_rmw_observations = self.create_rmw_update_from_er_subject(
                er_subject_name_to_subject_mapping[er_subject_id],
                er_subject_id_to_subjectsource_mapping[er_subject_id],
            )

        await self.rmw_client.upload_data(new_rmw_observations)

    async def create_rmw_update_from_er_subject(
        self, er_subject: dict, er_subjectsource: dict
    ) -> dict:
        """
        Create new updates from ER data for upload to RMWHub.
        """

        # Create traps list:
        traps = []
        if not er_subject.get("additional") or not er_subject.get("additional").get(
            "traps"
        ):
            logger.error(f"No traps found for set ID {er_subject.get('name')}.")
            return {}

        for device in er_subject.get("additional").get("devices"):
            traps.append(
                Trap(
                    id=er_subject.get("id"),
                    sequence=1 if device.get("label") == "a" else 2,
                    latitude=device.get("location").get("latitude"),
                    longitude=device.get("location").get("longitude"),
                    deploy_datetime_utc=device.get("last_updated")
                    if er_subject.get("is_active")
                    else None,
                    surface_datetime_utc=device.get("last_updated")
                    if er_subject.get("is_active")
                    else None,
                    retrieved_datetime_utc=device.get("last_updated")
                    if not er_subject.get("is_active")
                    else None,
                    status=Status.DEPLOYED
                    if er_subject.get("is_active")
                    else Status.RETRIEVED,
                )
            )

        # Create gear set:
        gear_set = GearSet(
            id=er_subjectsource.get("id"),
            deployment_type="trawl" if len(traps) > 1 else "single",
            traps_in_set=len(traps),
            trawl_path=None,
            share_with=["Earth Ranger"],
            traps=traps,
        )

        return gear_set

    def create_set_id_to_gearset_mapping(self, sets: List[GearSet]) -> dict:
        """
        Create a mapping of trap IDs to GearSets.
        """

        set_id_to_set_mapping = {}
        for gear_set in sets:
            set_id_to_set_mapping[gear_set.id] = gear_set
        return set_id_to_set_mapping

    @classmethod
    def clean_id_str(self, subject_name: str):
        """
        Resolve the subject name to the actual subject name
        """
        if not subject_name:
            logger.error("Cannot clean string. Subject name is empty.")
            return None

        cleaned_str = (
            subject_name.replace("device_", "")
            .replace("_0", "")
            .replace("_1", "")
            .replace("rmwhub_", "")
            .replace("e_", "")
            .replace("rmw_", "")
        )
        return cleaned_str


class RmwHubClient:
    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, api_key: str, rmw_url: str):
        self.api_key = api_key
        self.rmw_url = rmw_url

    async def search_hub(
        self, start_datetime_str: str, minute_interval: int, status: bool = None
    ) -> dict:
        """
        Downloads data from the RMWHub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")
        end_datetime = start_datetime + timedelta(minutes=minute_interval)
        end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "max_sets": 1000,
            # "status": "deployed",
            "from_latitude": -90,
            "to_latitude": 90,
            "from_longitude": -180,
            "to_longitude": 180,
            "start_deploy_utc": start_datetime_str,
            "end_deploy_utc": end_datetime_str,
            "start_retrieve_utc": start_datetime_str,
            "end_retrieve_utc": end_datetime_str,
        }

        if status:
            data["status"] = status

        url = self.rmw_url + "/search_hub/"

        response = requests.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text

    async def upload_data(self, sets: List) -> str:
        """
        Upload data to the RMWHub API using the upload_data endpoint.
        ref: https://ropeless.network/api/docs
        """

        url = self.rmw_url + "/upload_data/"

        upload_data = {
            "format_version": 0,
            "api_key": self.api_key,
            "sets": sets,
        }

        response = requests.post(url, headers=RmwHubClient.HEADERS, json=upload_data)

        if response.status_code != 200:
            logger.error(
                f"Failed to upload data to RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text
