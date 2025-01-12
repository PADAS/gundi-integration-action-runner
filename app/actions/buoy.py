import json
from typing import List
from erclient import ERClient
from datetime import datetime
import logging

import requests

from app.actions.rmwhub import GearSet

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

    # TODO: Validate include details works as expected
    async def get_er_subjects(self):
        filter = {
            "include_inactive": True,
            "updated_since": self._lower_bound.isoformat(),
            "include_details": True,
        }

        subjects = await self.er_client.get_subjects(filter=filter)

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

    async def update_status(self, er_subject: dict, rmw_set: GearSet):
        """
        Update the status of the ER subject based on the RMW status and deployment/retrieval times
        """

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
                await self.patch_er_subject_status(er_subject.get("id"), True)
            elif rmw_set.get("status") == "retrieved":
                await self.patch_er_subject_status(er_subject.get("id"), False)
        else:
            logger.error(
                f"Failed to compare gear set for trap ID {er_subject.get('id')}. ER last updated: {er_last_updated}, RMW deployed: {deployment_time}, RMW retrieved: {retrieval_time}"
            )

    async def patch_er_subject_status(self, er_subject_id: str, state: bool):
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
