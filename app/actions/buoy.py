import json
from typing import List
import logging

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
    async def get_er_subjects(self, start_datetime: str, end_datetime: str) -> List:

        updated_sice = start_datetime
        url = self.er_site + f"/subjects/?include_details=True&include_inactive=True"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"
        response = await requests.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            print("Request was successful")
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No subject sources found")
                return None
            return data["data"]
        else:
            logger.error(f"Failed to make request. Status code: {response.status_code}")

        return None

        return subjects

    async def get_er_subjectsources(self) -> List:
        """
        Get SubjectSource mapping
        """

        url = self.er_site + f"/subjectsources/?include_details=True"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"
        response = await requests.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            print("Request was successful")
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No subject sources found")
                return None
            return data["data"]["results"]
        else:
            logger.error(f"Failed to make request. Status code: {response.status_code}")

        return None

    async def patch_er_subject_status(self, er_subject_id: str, state: bool):
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        # TODO: Check if er_subject_id is not UUID, then find the subject by name

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
            subject_name.replace("device_", "")
            .replace("_0", "")
            .replace("_1", "")
            .replace("rmwhub_", "")
        )
        return cleaned_str
