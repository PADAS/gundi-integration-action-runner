import json
from typing import List
import logging

import httpx


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
            url += f"&position_updated_since={updated_since.isoformat()}"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No subjects found")
                return None
            logger.info(f"Request to get ER subjects was successful.  Loaded {len(data['data'])} subjects.")
            return data["data"]
        else:
            logger.error(f"Failed to make request. Status code: {response.status_code}")

        return []

    async def get_latest_observations(self, subject_id: str, page_size: int) -> dict:
        """
        Get the latest observations for a subject. Return only the latest observation when page_size = 1.
        """
        url = f"{self.er_site}/observations/"

        params = {
            "sort_by": "-recorded_at",
            "subject_id": subject_id,
            "include_details": "true",
            "page_size": page_size,
            "include_additional_data": "true",
        }
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=BuoyClient.headers, params=params)

        if response.status_code == 200:
            logger.info("Request to get latest observation was successful")
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No observations found")
                return []

            return data["data"]["results"]
        else:
            logger.error(
                f"Failed to get latest observation. Status code: {response.status_code}"
            )

        return []

    async def get_gear(self) -> List:
        """
        Get all gear
        """

        url = self.er_site + f"/gear/"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            logger.info("Request to get ER gear was successful")
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                logger.error(f"No gear found")
                return []
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

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=BuoyClient.headers)

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

        async with httpx.AsyncClient() as client:
            response = await client.patch(url, headers=BuoyClient.headers, json=dict)

        if response.status_code != 200:
            logger.exception(
                "Failed to update subject state for %s. Error: %s",
                er_subject_name,
                response.text,
            )
        logger.info(
            f"Successfully updated subject state for {er_subject_name} to {state}"
        )

    async def get_source_provider(self, er_subject_id: str) -> str:
        """
        Get the source provider for a subject
        """

        url = self.er_site + f"/subject/{er_subject_id}/sources/"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=BuoyClient.headers)

        if response.status_code == 200:
            logger.info("Request to get source provider was successful")
            data = json.loads(response.text)
            data = data.get("data")[0]
            if not data.get("provider"):
                logger.error(f"No source provider found")
            return data.get("provider")
        else:
            logger.error(
                f"Failed to get source provider. Status code: {response.status_code}"
            )

        return {}

    async def create_v1_observation(
        self, source_provider: str, observation: dict
    ) -> dict:
        """
        Create a new observation using the Gundi v1 Sensors API.
        """

        url = self.er_site + f"/sensors/generic/{source_provider}/status/"
        BuoyClient.headers["Authorization"] = f"Bearer {self.er_token}"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, headers=BuoyClient.headers, json=observation
            )

        if response.status_code == 201:
            logger.info("Request to create observation was successful")
            return 1
        else:
            logger.error(
                f"Failed to create observation. Status code: {response.status_code}"
            )

        return 0
