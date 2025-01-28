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
    async def get_er_subjects(self, start_datetime: str = None) -> List:

        updated_since = start_datetime
        url = self.er_site + f"/subjects/?include_details=True&include_inactive=True"
        if updated_since:
            url += f"&updated_since={updated_since}"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"
        response = requests.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            print("Request to get ER subjects was successful")
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No subjects found")
                return None
            return data["data"]
        else:
            logger.error(f"Failed to make request. Status code: {response.status_code}")

        return []

    async def get_er_subject_by_name(self, name: str) -> dict:

        url = (
            self.er_site
            + f"/subjects/?name={name}&include_details=True&include_inactive=True"
        )
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"
        response = requests.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            print(f"Request to get ER subject with name: {name} was successful")
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No subjects found")
                return None
            return data["data"]
        else:
            logger.error(
                f"Failed to get subject with name: {name}. Status code: {response.status_code}"
            )

        return []

    async def patch_er_subject_status(self, er_subject_name: str, state: bool):
        """
        Update the state of a subject by either the subject ID or the subject name
        """

        subject = await self.get_er_subject_by_name(er_subject_name)
        subject = subject[0] if subject else None
        if not subject:
            logger.error(f"Subject with name {er_subject_name} not found")
            return

        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"
        url = self.er_site + f"/subject/{subject.get('id')}/"

        dict = {"is_active": state}
        response = requests.patch(url, headers=BuoyClient.headers, json=dict)
        if response.status_code != 200:
            logger.exception(
                "Failed to update subject state for %s. Error: %s",
                er_subject_name,
                response.text,
            )
        logger.info(
            f"Successfully updated subject state for {er_subject_name} to {state}"
        )
