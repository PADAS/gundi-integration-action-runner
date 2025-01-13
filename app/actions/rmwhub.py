from datetime import datetime, timedelta
from enum import Enum
from typing import List

from app.actions.buoy import BuoyClient
import hashlib
import logging
import json
import pydantic
import requests

logger = logging.getLogger(__name__)


class Status(Enum):
    DEPLOYED = "gear_deployed"
    RETRIEVED = "gear_retrieved"


class Trap(pydantic.BaseModel):
    trap_id: str
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
                self.trap_id,
                self.sequence,
                self.latitude,
                self.longitude,
                self.deploy_datetime_utc,
            )
        )

    def get_latest_update_time(self):
        """
        Get the last updated time of the trap.
        """

        deployment_time = datetime.strptime(
            self.deploy_datetime_utc, "%Y-%m-%dT%H:%M:%S"
        )
        retrived_time = datetime.strptime(
            self.retrieved_datetime_utc, "%Y-%m-%dT%H:%M:%S"
        )

        if deployment_time > retrived_time:
            last_updated = deployment_time
        else:
            last_updated = retrived_time

        return str(last_updated)


class GearSet(pydantic.BaseModel):
    vessel_id: str
    set_id: str
    deployment_type: str
    traps_in_set: int
    trawl_path: str
    share_with: List[str]
    traps: List[Trap]

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key):
        return self.__getitem__(key)

    def __hash__(self):
        return hash((self.set_id, self.deployment_type, tuple(self.traps)))

    def get_devices(self) -> List:
        """
        Get the devices info for the gear set.
        """

        devices = []
        for trap in self.traps:
            devices.append(
                {
                    "device_id": trap.get("trap_id") + "_" + str(trap.get("sequence")),
                    "label": "a" if trap.get("sequence") == 1 else "b",
                    "location": {
                        "latitude": trap.get("latitude"),
                        "longitude": trap.get("longitude"),
                    },
                }
            )

        return devices

    def create_observations_for_gearset(self) -> List:
        """
        Create observations for the gear set.
        """

        devices = self.get_devices()

        observations = []
        for trap in self.traps:
            observations.append(
                self.create_observation_for_event(trap, devices, Status.DEPLOYED)
            )
            observations.append(
                self.create_observation_for_event(trap, devices, Status.RETRIEVED)
            )

        logger.info(
            f"Created {len(observations)} observations for gear set {self.get('set_id')}."
        )

        return observations

    def create_observation_for_event(
        self, trap: Trap, devices: List, event_type: Status
    ) -> dict:
        """
        Create an observation from the RMW Hub trap.
        """

        display_id_hash = hashlib.sha256(str(self.get("set_id")).encode()).hexdigest()[
            :12
        ]
        subject_name = "rmwhub_" + trap.get("trap_id")

        last_updated = trap.get_latest_update_time()
        # TODO: solve not being able to control Trap ID (Subject ID) upon creation in ER
        # Question: Use name to store Trap ID? Or use a different field?
        # Solution: Add both names and IDs to ER ID/name to subject mapping?
        # Use Name = Trap ID to create new subjects from RMWHub data and Subject ID to update existing subjects.
        observation = {
            "name": subject_name,
            "source": subject_name,
            "type": RmwHubAdapter.SOURCE_TYPE,
            "subject_type": RmwHubAdapter.SUBJECT_SUBTYPE,
            "recorded_at": last_updated,
            "location": {"lat": trap.get("latitude"), "lon": trap.get("longitude")},
            "additional": {
                "subject_name": subject_name,
                "rmwhub_id": self.get("set_id"),
                "display_id": display_id_hash,
                "event_type": str(event_type),
                "devices": devices,
            },
        }

        logger.info(
            f"Created observation for trap ID {trap.get('trap_id')} with event type {event_type}."
        )

        return observation


class RmwSets(pydantic.BaseModel):
    sets: List[GearSet]


class RmwHubAdapter:

    SOURCE_TYPE = "ropeless_buoy"
    SUBJECT_SUBTYPE = "ropeless_buoy_device"
    EVENT_TYPE = "gear_deployed"

    def __init__(self, api_key: str, rmw_url: str, er_token: str, er_destination: str):
        self.rmw_client = RmwHubClient(api_key, rmw_url)
        self.er_client = BuoyClient(er_token, er_destination)

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

        sets = response_json["sets"]

        return sets

    async def process_sets(
        self, sets: RmwSets, start_datetime_str: str, minute_interval: int
    ) -> List:
        """
        Process the sets from the RMW Hub API.
        """

        # Normalize the extracted data into a list of observations following to the Gundi schema:
        observations = []

        # Download Data from ER for the same time interval as RmwHub
        # TODO: Only download recent updates from ER
        er_subjects = await self.er_client.get_er_subjects(start_datetime_str)

        # Create lists of er_subject_names and rmw_trap_ids/set_ids
        er_subject_id_to_subject_mapping = dict(
            (subject.get("id"), subject) for subject in er_subjects
        )
        er_subject_id_to_subject_mapping.update(
            dict((subject.get("name"), subject) for subject in er_subjects)
        )

        subjects_in_er = set(er_subject_id_to_subject_mapping.keys())
        er_subjectsources = await self.er_client.get_er_subjectsources()
        subject_id_to_subjectsource_mapping = dict(
            (subjectsource.get("subject"), subjectsource)
            for subjectsource in er_subjectsources
        )
        subject_id_to_subjectsource_mapping.update(
            dict(
                (
                    self.er_client.resolve_subject_name(
                        subject_id_to_subjectsource_mapping[
                            subjectsource.get("subject")
                        ].get("name")
                    ),
                    subjectsource,
                )
                for subjectsource in er_subjectsources
            )
        )

        rmw_trap_id_to_set_mapping = self.create_trap_to_gearset_mapping(sets=sets)
        traps_in_rmw = set(rmw_trap_id_to_set_mapping.keys())

        visited = set()
        upload_to_rmw = set()

        # Compare data:
        #   For each set in ER:
        for er_subject_id in subjects_in_er:
            # Check if the set ID is in the Rmw sets
            # TODO: Check if the trap ID or the subject name is in visited so that traps are not visited twice and uploaded twice to RMW
            if er_subject_id in traps_in_rmw and er_subject_id not in visited:
                # If yes, add all traps in current gearset to list of "visited" set IDs.
                gearset = rmw_trap_id_to_set_mapping[er_subject_id]
                visited.add(gearset.traps[0].trap_id)
                visited.add(gearset.traps[1].trap_id)
                logger.info(
                    f"Earthranger Trap ID {er_subject_id} found in RMW sets for datetime: {start_datetime_str} and interval {minute_interval}."
                )

                # Create observations for the gear set from RmwHub
                new_observations = await self.create_observations(
                    subjects_in_er[er_subject_id], traps_in_rmw[er_subject_id]
                )

                observations.append(new_observations)
                logger.info(
                    f"Processed {len(new_observations)} new observations for trap ID {er_subject_id}."
                )

                if len(new_observations) == 0:
                    # New observations dict will be empty if ER has the latest update
                    logger.info(
                        f"ER has the most recent update for trap ID {er_subject_id}."
                    )

                # Update status of ER Trap, if necessary
                self.er_client.update_status(
                    subjects_in_er[er_subject_id], traps_in_rmw[er_subject_id]
                )
            else:
                # If no, mark for upload to RMW and continue
                upload_to_rmw.add(er_subject_id)
                logger.info(
                    f"Trap ID {er_subject_id} not found in RMW sets for datetime: {start_datetime_str} and interval {minute_interval}. Mark for upload to RMwHub."
                )
        #   For each Rmw trap ID that was not in ER (not in "visited"), post new observations:
        for trap_id in traps_in_rmw - visited:
            # Create new observations for traps in RMW
            gearset = rmw_trap_id_to_set_mapping[trap_id]
            observations.append(gearset.create_observations_for_gearset())

            logger.info(
                f"Trap ID {trap_id} not found in ER sets for datetime: {start_datetime_str} and interval {minute_interval}. New observations created for RMW trap."
            )

        # Process upload data to RmwHub
        await self.upload_data(
            upload_to_rmw,
            er_subject_id_to_subject_mapping,
            subject_id_to_subjectsource_mapping,
        )

        return observations

    async def create_observations(
        self, trap_id: str, er_subject: dict, rmw_set: GearSet
    ) -> List:
        """
        Create new observations for ER from RmwHub data.

        Returns an empty list if ER has the most recent updates. Otherwise, list of new observations to write to ER.
        """

        # Determine which trap matches the er_subject:
        trap_to_update = None
        for trap in rmw_set.traps:
            if trap_id == trap.get("trap_id"):
                trap_to_update = trap

        # If locations in ER and rmw match, no updates are needed
        # TODO: Is this check needed?
        if er_subject.get("latitude") == trap_to_update.get(
            "latitude"
        ) and er_subject.get("longitude") == trap_to_update.get("longitude"):
            return []

        # Create observations for the gear set
        observations = rmw_set.create_observations_for_gearset()

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
                    trap_id=er_subject.get("id"),
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
            set_id=er_subjectsource.get("id"),
            deployment_type="trawl" if len(traps > 1) else "single",
            traps_in_set=len(traps),
            trawl_path=None,
            share_with=["Earth Ranger"],
            traps=traps,
        )

        return gear_set

    def create_trap_to_gearset_mapping(self, sets: List[GearSet]) -> dict:
        """
        Create a mapping of trap IDs to GearSets.
        """

        trap_id_to_set_mapping = {}
        for gear_set in sets:
            for trap in gear_set.traps:
                trap_id_to_set_mapping[trap.trap_id] = gear_set
        return trap_id_to_set_mapping

    async def update_status(self, er_subject: dict, rmw_set: GearSet):
        """
        Update the status of the ER subject based on the RMW status and deployment/retrieval times
        """

        # TODO: Check if the the ID being checked for is UUID, if not find subject by name
        # Determine if ER or RMW has the most recent update in order to update status in ER:
        datetime_str_format = "%Y-%m-%dT%H:%M:%S"
        er_last_updated = datetime.strptime(
            er_subject.get("updated_at"), datetime_str_format
        )
        deployment_time = datetime.strptime(
            rmw_set.get("deploy_datetime_utc"), datetime_str_format
        )
        retrieval_time = datetime.strptime(
            rmw_set.get("retrieved_datetime_utc"), datetime_str_format
        )

        if er_last_updated > deployment_time and er_last_updated > retrieval_time:
            return
        elif er_last_updated < deployment_time or er_last_updated < retrieval_time:
            if rmw_set.get("status") == "deployed":
                await self.er_client.patch_er_subject_status(er_subject.get("id"), True)
            elif rmw_set.get("status") == "retrieved":
                await self.er_client.patch_er_subject_status(
                    er_subject.get("id"), False
                )
        else:
            logger.error(
                f"Failed to compare gear set for trap ID {er_subject.get('id')}. ER last updated: {er_last_updated}, RMW deployed: {deployment_time}, RMW retrieved: {retrieval_time}"
            )


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

        response = await requests.post(
            url, headers=RmwHubClient.HEADERS, json=upload_data
        )

        if response.status_code != 200:
            logger.error(
                f"Failed to upload data to RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text
