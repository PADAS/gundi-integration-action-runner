import json
from typing import List
import logging
import uuid

import requests


logger = logging.getLogger(__name__)


class BuoyClient:
    headers = {
        "Authorization": f"Bearer ",
    }

    def __init__(self, er_token: str, er_site: str):
        self.er_token = er_token
        self.er_site = er_site

    # TODO: Validate include details works as expected
    async def get_er_subjects(self, start_datetime: str = None) -> List:

        updated_since = start_datetime
        url = self.er_site + f"/subjects/?include_details=True&include_inactive=True"
        if updated_since:
            url += f"&updated_since={updated_since}"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"
        response = requests.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            print("Request was successful")
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No subject sources found")
                return None
            return data["data"]
        else:
            logger.error(f"Failed to make request. Status code: {response.status_code}")

        return []

    async def patch_er_subject_status(self, er_subject_id: str, state: bool):
        """
        Update the state of a subject by either the subject ID or the subject name
        """

        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        # TODO: Check if er_subject_id is not UUID, then find the subject by name
        try:
            uuid_obj = uuid.UUID(er_subject_id)
            url = self.er_site + f"/subject/{er_subject_id}"
        except ValueError:
            url = self.er_site + f"/subject/?name={er_subject_id}"

        dict = {"is_active": state}
        response = requests.patch(url, headers=BuoyClient.headers, json=dict)
        if response.status_code != 200:
            logger.exception(
                "Failed to update subject state for %s. Error: %s",
                er_subject_id,
                response.text,
            )

    def clean_subject_name(self, subject_name: str):
        """
        Resolve the subject name to the actual subject name
        """

        cleaned_str = (
            subject_name.replace("device_", "")
            .replace("_0", "")
            .replace("_1", "")
            .replace("rmwhub_", "")
        )
        return cleaned_str
