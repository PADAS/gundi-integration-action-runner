import logging
from fastapi import APIRouter, BackgroundTasks, Request
from app.services.webhooks import process_webhook
from app import settings

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "",
    summary="Process Webhooks from third-party systems",
)
async def webhooks(
    request: Request,
    background_tasks: BackgroundTasks
):
    body = await request.body()
    print(f"Message Received through Webhooks. RAW body: {body}")
    headers = dict(request.headers)
    print(f"Headers: {headers}")
    if settings.PROCESS_WEBHOOKS_IN_BACKGROUND:
        background_tasks.add_task(
            process_webhook,
            request=request,
        )
        return {}
    else:
        return await process_webhook(
            request=request,
        )
