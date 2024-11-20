import asyncio
import json
from typing import List
from datetime import datetime, timezone, timedelta
from gql import gql, Client
from gql.transport.httpx import HTTPXAsyncTransport
import httpx
import pydantic

class User(pydantic.BaseModel):
    user_id: str
    client_id: str
    contact_id: str
    client_name: str

class AuthenticateResult(pydantic.BaseModel):
    users: List[User] = pydantic.Field(default_factory=list, alias="selectUsersByUsernamePassword")


class UserAsset(pydantic.BaseModel):
    unit_id: str
    user_id: str
    asset_id: str
    currentLoc: dict


class AssetsResult(pydantic.BaseModel):
    userAssets: List[UserAsset] = pydantic.Field(default_factory=list)


class HistoryItem(pydantic.BaseModel):
    device_timezone: int
    unit_id: str
    fixtime: datetime
    alerts: List[str]
    location: str
    speed: int
    course: int
    longitude: float
    latitude: float
    reg_no: str
    driver: str

    @pydantic.validator("fixtime", pre=True)
    def _fixtime(v):
        return datetime.strptime(v, "%m/%d/%Y %I:%M:%S %p").replace(tzinfo=timezone.utc)


class HistoryResult(pydantic.BaseModel):
    response: str # "success" or somehthing else.
    data: List[HistoryItem]

async def authenticate(username: str, password: str):
    # Select your transport with a defined url endpoint
    transport = HTTPXAsyncTransport(url="https://graphql.bluetrax.co.ke/graphql/")

    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as session:

        query = gql(
            """
            query selectUsersByUsernamePassword($user_name: String!, $password: String!) {
                selectUsersByUsernamePassword(user_name: $user_name, password: $password) {
                        user_id
                        client_id
                        contact_id
                        client_name
                        __typename
                }
            }   
        """
        )

        # Execute the query on the transport
        result = await session.execute(query, variable_values={"user_name": username, "password": password})
        if result:
            return AuthenticateResult(**result)
        else:
            return None


async def get_assets(user_id: str):
    transport = HTTPXAsyncTransport(url="https://graphql.bluetrax.co.ke/graphql/")

    user_id = int(user_id)
    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as session:
        query = gql(
            """
        query q($user_id: Int!) {
            userAssets(where: {user_id: {eq: $user_id} }) {
                unit_id
                user_id
                asset_id
                currentLoc {
                    gps_event
                    local_fixtime
                    location
                }
                }
            }
            """
        )
        result = await session.execute(query, variable_values={"user_id": user_id})
        return AssetsResult(**result)


async def get_asset_history(unit_id: str, start_time: datetime, end_time: datetime):
    q = {
            "api_action":"get_history",
            "unit_id": unit_id, 
          "start_date": start_time.isoformat(),
          "end_date": end_time.isoformat()
          }
    
    async with httpx.AsyncClient() as client:
        r = await client.get("https://rest.bluetrax.co.ke/AnalyticsService", 
                             params={'request': json.dumps(q)}
        )
        if r.status_code == 200:
            return HistoryResult(**r.json())
        else:
            return None

if __name__ == "__main__":
    auth_result = asyncio.run(authenticate("***REMOVED***", "***REMOVED***"))
    # print(val)

    assets = asyncio.run(get_assets(auth_result.users[0].user_id))
    end_date = datetime.now(tz=timezone.utc)
    val = asyncio.run(get_asset_history(int(assets.userAssets[0].unit_id), end_date - timedelta(minutes=20), end_date))
    
    print(val.json(indent=2))