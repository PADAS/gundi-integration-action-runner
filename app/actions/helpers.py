from gundi_client_v2 import GundiClient

_client = GundiClient()
headers = {
    "Authorization": f"Bearer ",
}


def get_er_subject(name: str) -> dict:
    url = self.er_site + f"/subjects/?name={subject_name}&include_inactive=true"
    response = requests.get(url, headers=self.headers)
    if response.status_code == 200:
        data = json.loads(response.text)
        if len(data["data"]) == 0:
            logger.error(f"No subject found for {subject_name}")
            return None
        for subject in data["data"]:
            if subject["name"] == subject_name:
                return subject
        return None
    else:
        logger.error(
            f"Failed to get subject for {subject_name}. Error: {response.status_code} - {response.text}"
        )
        return None


def patch_er_subject_status(er_token: str, er_subject_id: str, state: bool):
    # TODO: Get ER token from GundiClient API
    headers["Authorization"] = f"Bearer {er_token}"

    url = self.er_site + f"/subject/{er_subject_id}"
    dict = {"is_active": state}
    response = requests.patch(url, headers=headers, json=dict)
    if response.status_code != 200:
        logger.exception(
            "Failed to update subject state for %s. Error: %s",
            er_subject,
            response.text,
        )


def get_er_token_and_site():
    connection_details = await _client.get_connection_details(integration_id)

    for destination in connection_details.destinations:
        destination_details = await _client.get_integration_details(destination.id)
