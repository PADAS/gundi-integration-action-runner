from typing import List, Set, Tuple
from datetime import datetime, timedelta
import hashlib
import json
import pydantic
import requests
import os
from .configurations import PullRmwHubObservationsConfiguration
from dotenv import load_dotenv


class RmwUpdates(pydantic.BaseModel):
    sets: List[Set]


class Trap(pydantic.BaseModel):
    sequence: int
    latitude: float
    longitude: float


class Set(pydantic.BaseModel):
    set_id: str
    deployment_type: str
    traps: List[Trap]


HEADERS = {"accept": "application/json", "Content-Type": "application/json"}


class RmwHubAdapter:
    def __init__(self, api_key: str, rmw_url: str):
        self.api_key = api_key
        self.rmw_url = rmw_url

    def download_data(
        self,
        action_config: PullRmwHubObservationsConfiguration,
        start_datetime_str: str,
    ) -> Tuple[RmwUpdates, List]:
        """
        Downloads data from the RMW Hub API using the search_others endpoint.
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

        response = requests.post(self.rmw_url, headers=HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        response_json = json.loads(response.text)
        updates = response_json["updates"]["sets"]
        deletes = response_json["deletes"]["sets"]

        return updates, deletes

    def process_updates(self, updates: RmwUpdates) -> List:
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
                    "manufacturer_id": "rmwhub_" + update.get("set_id") + "_0",
                    "source_type": source_type,
                    "subject_name": subject_name,
                    "subject_sub_type": subject_subtype,
                    "recorded_at": last_updated,
                    "location": {"lat": latitude, "lon": longitude},
                    "additional": {
                        "radio_state": "online-gps",
                        "rmwHub_id": update.get("set_id"),
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
                        "manufacturer_id": "rmwhub_" + update.get("set_id") + "_1",
                        "source_type": source_type,
                        "subject_name": subject_name,
                        "subject_sub_type": subject_subtype,
                        "recorded_at": last_updated,
                        "location": {"lat": latitude, "lon": longitude},
                        "additional": {
                            "radio_state": "online-gps",
                            "rmwHub_id": update.get("set_id"),
                            "display_id": display_id_hash,
                            "event_type": event_type,
                            "devices": devices,
                        },
                    }
                )

                # TODO; Write trawl line event to ER, not required for beta

        return observations

    def process_deletes(self, deletes):
        """
        Process the deletes from the RMW Hub API.
        """
        pass

    def upload_data():
        pass

    def delete_data():
        pass

    def sync_data():
        pass
