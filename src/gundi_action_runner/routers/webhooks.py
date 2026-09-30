import logging
from fastapi import APIRouter, BackgroundTasks, Request
from gundi_action_runner.services.webhooks import process_webhook
from gundi_action_runner import settings

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
    # Pre-read (and thereby cache) the body so background-task processing can
    # still access it after the response is returned. Never log its contents —
    # webhook payloads and headers can carry credentials and PII.
    body = await request.body()
    logger.debug(f"Webhook request received ({len(body)} bytes).")
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
