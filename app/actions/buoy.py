import json
from erclient import ERClient
from datetime import datetime
import logging

import requests

logger = logging.getLogger(__name__)


class BuoyClient:
    headers = {
        "Authorization": f"Bearer ",
    }

    def __init__(
        self, er_token: str, er_site: str, start_date: datetime, end_date: datetime
    ):
        self.er_token = er_token
        self.er_site = er_site
        self.er_client = ERClient(service_root=er_site, token=self.er_token)

        self._upper_bound = start_date
        self._lower_bound = end_date

    # TODO: Test in postman
    async def get_er_subjects(self):
        filter = {
            "update_date": {
                "upper": self._upper_bound.isoformat(),
                "lower": self._lower_bound.isoformat(),
            },
            # TODO: Need to set include_inactive=true
            # 'state': ['active', 'new'],
            "subject_subtype": "ropeless_buoy_device",
        }

        subjects = await self.er_client.get_subjects(filter=filter)

        return subjects

    def get_er_subject(self, name: str) -> dict:
        url = self.er_site + f"/subjects/?name={name}&include_inactive=true"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"
        response = requests.get(url, headers=BuoyClient.headers)
        if response.status_code == 200:
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No subject found for {name}")
                return None
            for subject in data["data"]:
                if subject["name"] == name:
                    return subject
            return None
        else:
            logger.error(
                f"Failed to get subject for {name}. Error: {response.status_code} - {response.text}"
            )
            return None

    async def patch_er_subject_status(self, er_subject_id: str, state: bool):
        # TODO: Get ER token from GundiClient API
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        url = self.er_site + f"/subject/{er_subject_id}"
        dict = {"is_active": state}
        response = await requests.patch(url, headers=BuoyClient.headers, json=dict)
        if response.status_code != 200:
            logger.exception(
                "Failed to update subject state for %s. Error: %s",
                er_subject_id,
                response.text,
            )

    def resolve_subject_name(self, subject_name: str):
        """
        Resolve the subject name to the actual subject name
        """

        cleaned_str = (
            subject_name.replace("device_", "").replace("_0", "").replace("_1", "")
        )
        return cleaned_str
