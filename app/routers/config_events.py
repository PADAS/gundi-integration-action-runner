import base64
import json
import logging
from fastapi import APIRouter, BackgroundTasks, Request
from app.services.config_events_consumer import process_config_event


logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/",
    summary="This endpoint processed configuration events from the portal",
)
async def process_request(
    request: Request,
):
    # Parse PubSub message
    json_data = await request.json()
    pubsub_message = json_data["message"]
    message_data = pubsub_message.get("data", "")
    decoded_data = base64.b64decode(message_data.encode("utf-8"))
    event_data = json.loads(decoded_data) if decoded_data else {}
    attributes = pubsub_message.get("attributes")
    return await process_config_event(event_data, attributes)


