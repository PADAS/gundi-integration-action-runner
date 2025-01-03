from datetime import datetime, timedelta
import hashlib
import logging
import json
from typing import List, Tuple
import pydantic
import requests
from app.actions.buoy import BuoyClient

logger = logging.getLogger(__name__)


class TrapSearchOthers(pydantic.BaseModel):
    sequence: int
    latitude: float
    longitude: float

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key):
        return self.__getitem__(key)

    def __hash__(self):
        return hash((self.sequence, self.latitude, self.longitude))


class GearSetSearchOthers(pydantic.BaseModel):
    set_id: str
    deployment_type: str
    traps: List[TrapSearchOthers]

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key):
        return self.__getitem__(key)

    def __hash__(self):
        return hash((self.set_id, self.deployment_type, tuple(self.traps)))


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


class RmwUpdates(pydantic.BaseModel):
    sets: List[GearSetSearchOthers]


class RmwSets(pydantic.BaseModel):
    sets: List[GearSet]


class RmwHubAdapter:
    def __init__(self, api_key: str, rmw_url: str, er_site: str, er_token: str):
        self.rmw_client = RmwHubClient(api_key, rmw_url)
        # TODO: Get ER site and token using logic in helpers.py
        self.er_client = BuoyClient(er_token, er_site)

    def download_data_search_hub(
        self, start_datetime_str: str, minute_interval: int, status: bool = None
    ) -> RmwSets:
        """
        Downloads data from the RMW Hub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        response = self.rmw_client.search_hub(
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
        er_subjects = await self.er_client.get_er_subjects()

        # Download Data from Rmw from search_hub endpoint
        rmw_sets = await self.rmw_client.search_hub(
            start_datetime_str=start_datetime_str, minute_interval=minute_interval
        )

        # Create lists of er_subject_names and rmw_trap_ids/set_ids
        er_subject_name_to_subject_mapping = dict(
            (self.er_client.resolve_subject_name(subject["name"]), subject)
            for subject in er_subjects
        )
        subjects_in_er = set(er_subject_name_to_subject_mapping.keys())

        rmw_trap_name_to_set_mapping = self.create_trap_to_set_mapping(sets=rmw_sets)
        traps_in_rmw = set(rmw_trap_name_to_set_mapping.keys())

        visited = set()
        update_rmw = set()

        # Compare data:
        #   For each set in ER:
        for er_subject_name in subjects_in_er:
            # Check if the set ID is in the Rmw sets
            if er_subject_name in traps_in_rmw:
                # If yes, add to list of "visited" set IDs.
                visited.add(er_subject_name)
                logger.info(
                    f"Set ID {er_subject_name} found in RMW sets for datetime: {start_datetime_str} and interval {minute_interval}."
                )
                # Determine if ER or RMW has the most recent update:
                # If rmw has most recent update, update ER set
                # Log it
            else:
                # If no, continue
                update_rmw.add(er_subject_name)
                logger.info(
                    f"Set ID {er_subject_name} not found in RMW sets for datetime: {start_datetime_str} and interval {minute_interval}."
                )
        #   For each Rmw trap ID that was not in ER (not in "visited"):
        for er_subject_name in update_rmw:
            # Add new observation for current Trap ID in ER
            pass
        # Send new observations to ER

    # Process upload data to RmwHub

    def download_data_search_others(
        self,
        start_datetime_str: str,
    ) -> Tuple[RmwUpdates, List]:
        """
        Downloads data from the RMW Hub API using the search_others endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        response = self.rmw_client.search_others(start_datetime_str)
        response_json = json.loads(response)

        updates = response_json["updates"]["sets"]
        deletes = response_json["deletes"]["sets"]

        return updates, deletes

    def process_updates_search_others(self, updates: RmwUpdates) -> List:
        """
        Process the updates from the RMW Hub API.
        """

        # Normalize the extracted data into a list of observations following to the Gundi schema:
        # TODO: Determine if gearset is already in DB (implement sync function)
        observations = []
        source_type = "ropeless_buoy"
        subject_subtype = "ropeless_buoy_device"
        event_type = "gear_deployed"
        last_updated = datetime.now().isoformat()

        # Create observations
        for update in updates:
            display_id_hash = hashlib.sha256(
                str(update.get("set_id")).encode()
            ).hexdigest()[:12]
            subject_name = "rmwhub_" + update.get("set_id")

            # Create devices string
            devices = []
            for trap in update.get("traps"):
                devices.append(
                    {
                        "last_updated": last_updated,
                        "device_id": subject_name + "_" + str(trap.get("sequence")),
                        "label": "a" if trap.get("sequence") == 0 else "b",
                        "location": {
                            "latitude": trap.get("latitude"),
                            "longitude": trap.get("longitude"),
                        },
                    }
                )

            subject_name = subject_name + "_0"
            latitude = update.get("traps")[0].get("latitude")
            longitude = update.get("traps")[0].get("longitude")
            observations.append(
                {
                    "name": update.get("set_id"),
                    "source": "rmwhub_" + update.get("set_id") + "_0",
                    "type": source_type,
                    "subject_type": subject_subtype,
                    "recorded_at": last_updated,
                    "location": {"lat": latitude, "lon": longitude},
                    "additional": {
                        "subject_name": subject_name,
                        "rmwhub_id": update.get("set_id"),
                        "display_id": display_id_hash,
                        "event_type": event_type,
                        "devices": devices,
                    },
                }
            )

            if update.get("deployment_type") == "trawl":
                subject_name = subject_name + "_1"
                latitude = update.get("traps")[1].get("latitude")
                longitude = update.get("traps")[1].get("longitude")
                observations.append(
                    {
                        "name": update.get("set_id"),
                        "source": "rmwhub_" + update.get("set_id") + "_1",
                        "type": source_type,
                        "subject_type": subject_subtype,
                        "recorded_at": last_updated,
                        "location": {"lat": latitude, "lon": longitude},
                        "additional": {
                            "subject_name": subject_name,
                            "rmwhub_id": update.get("set_id"),
                            "display_id": display_id_hash,
                            "event_type": event_type,
                            "devices": devices,
                        },
                    }
                )

                # TODO; Write trawl line event to ER, not required for beta

        return observations

    def upload_data():
        pass

    def delete_data():
        pass

    def sync_data():
        pass

    def create_trap_to_set_mapping(self, sets: List[GearSet]) -> dict:
        trap_name_to_set_mapping = {}
        for gear_set in sets:
            for trap in gear_set.traps:
                trap_name_to_set_mapping[trap.trap_id] = gear_set
        return trap_name_to_set_mapping


class RmwHubClient:
    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, api_key: str, rmw_url: str):
        self.api_key = api_key
        self.rmw_url = rmw_url

    def search_others(self, start_datetime_str: str) -> dict:
        """
        Downloads data from the RMWHub API using the search_others endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "start_datetime_utc": start_datetime_str,
            "from_latitude": -90,
            "to_latitude": 90,
            "from_longitude": -180,
            "to_longitude": 180,
            "include_own": False,
        }

        url = self.rmw_url + "/search_others/"

        response = requests.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text

    async def search_hub(
        self, start_datetime_str: str, minute_interval: int, status: bool = None
    ) -> dict:
        """
        Downloads data from the RMWHub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%dT%H:%M:%S")
        end_datetime = start_datetime + timedelta(minutes=minute_interval)
        end_datetime_str = end_datetime.strftime("%Y-%m-%dT%H:%M:%S")

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "max_sets": 1000,
            "status": "deployed",
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

        response = await requests.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text
