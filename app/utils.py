
import aiohttp
from gcloud.aio.storage import Storage


async def store(*, bucket_name: str, destination_blob_name: str, data: str):

    async with aiohttp.ClientSession() as session:
        client = Storage(session=session)
        status = await client.upload(bucket_name, destination_blob_name, file_data=data, content_type='application/json')
        print(status)

