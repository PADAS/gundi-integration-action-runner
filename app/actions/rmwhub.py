from datetime import datetime, timedelta
import pydantic
import requests
import os
from dotenv import load_dotenv


class RmwUpdates(pydantic.BaseModel):
    sets: List[Set]


class Set(pydantic.BaseModel):
    set_id: string
    deployment_type: string
    traps: List[Trap]


class Trap(pydantic.BaseModel):
    sequence: int
    latitude: float
    longitude: float


HEADERS = {"accept": "application/json", "Content-Type": "application/json"}


class RmwHubAdapter:
    def __init__(self):
        load_dotenv()
        self.RMW_URL = os.getenv("RMW_URL")
        self.API_KEY = os.getenv("RMW_API_KEY")

    def download_data(
        self,
        action_config: PullRmwHubObservationsConfiguration,
        start_datetime_str: string,
    ) -> Tuple[RmwUpdates, List]:
        """
        Downloads data from the RMW Hub API using the search_others endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        data = {
            "format_version": 0.1,
            "api_key": self.API_KEY,
            "start_datetime_utc": start_datetime_str,
            "from_latitude": -90,
            "to_latitude": 90,
            "from_longitude": -180,
            "to_longitude": 180,
            "include_own": False,
        }

        response = requests.post(self.RMW_URL, headers=HEADERS, json=data)

        updates = RmwUpdates(response.json()["updates"]["sets"])
        deletes = response.json()["deletes"]["sets"]

        return updates, deletes

    def process_updates(updates: RmwUpdates) -> List:
        """
        Process the updates from the RMW Hub API.
        """

        # Normalize the extracted data into a list of observations following to the Gundi schema:
        for update in updates:
            observations = [
                {
                    "source": "collar-xy123",
                    "type": "tracking-device",
                    "subject_type": "puma",
                    "recorded_at": "2024-01-24 09:03:00-0300",
                    "location": {"lat": -51.748, "lon": -72.720},
                    "additional": {"speed_kmph": 10},
                }
            ]
            pass

    def process_deletes(deletes):
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
