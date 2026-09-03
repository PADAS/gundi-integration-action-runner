import base64
import json
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from app.routers import actions, webhooks, config_events
import app.settings as settings
from fastapi.middleware.cors import CORSMiddleware

from app.services.action_runner import execute_action, _portal
from app.services.self_registration import register_integration_in_gundi
from app.services.webhooks import close_diagnostic_client


# For running behind a proxy, we'll want to configure the root path for OpenAPI browser.
root_path = os.environ.get("ROOT_PATH", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup Hook
    if settings.REGISTER_ON_START:
        await register_integration_in_gundi(gundi_client=_portal)
        # ToDo: set env var to false in GCP after registration
    yield
    # Shutdown Hook
    await _portal.close()
    await close_diagnostic_client()


app = FastAPI(
    title="Gundi Integration Actions Execution Service",
    description="API to trigger actions against third-party systems",
    version="1",
    lifespan=lifespan
)

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger(__name__)


@app.get(
    "/",
    tags=["health-check"],
    summary="Check that the service is healthy",
    description="This is primarily used to test authentication. It allows a caller to see whether it has successfully authenticated or is identified as _anonymous_.",
)
def read_root(
    request: Request,
):
    return {"status": "healthy"}


@app.post(
    "/",
    summary="Execute an action from GCP PubSub",
)
async def execute(
    request: Request,
    background_tasks: BackgroundTasks
):
    json_data = await request.json()
    logger.debug(f"JSON: {json_data}")
    payload = base64.b64decode(json_data["message"]["data"]).decode("utf-8").strip()
    json_payload = json.loads(payload)
    logger.debug(f"JSON Payload: {json_payload}")
    # `triggered_by` lets the portal mark how the run was initiated (e.g. a
    # scheduled tick vs an operator's "Run now"). Absent the marker we default
    # to automated, so scheduled pulls on destination-only integrations skip
    # quietly instead of erroring.
    #
    # It is read from the PubSub message attributes as well as the body:
    # gundi_core's RunIntegrationAction command has no `triggered_by` field, so
    # a portal that serializes that model cannot put the marker in the payload
    # and the MANUAL branch would never be reachable over PubSub.
    triggered_by = json_payload.get("triggered_by") or (
        json_data["message"].get("attributes") or {}
    ).get("triggered_by")
    if settings.PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND:
        background_tasks.add_task(
            execute_action,
            integration_id=json_payload.get("integration_id"),
            action_id=json_payload.get("action_id"),
            config_overrides=json_payload.get("config_overrides"),
            triggered_by=triggered_by,
        )
    else:
        await execute_action(
            integration_id=json_payload.get("integration_id"),
            action_id=json_payload.get("action_id"),
            config_overrides=json_payload.get("config_overrides"),
            triggered_by=triggered_by,
        )
    return {}


@app.post(
    "/push-data",
    summary="Process messages from PubSub and run push actions",
)
async def push_data(
    request: Request,
):
    json_body = await request.json()
    logger.debug(f"JSON: {json_body}")
    payload = base64.b64decode(json_body["message"]["data"]).decode("utf-8").strip()
    logger.debug(f"Payload: {payload}")
    json_payload = json.loads(payload)
    attributes = json_body["message"].get("attributes", {})
    logger.debug(f"Attributes: {attributes}")
    destination_id = attributes.get("destination_id")
    if not destination_id:
        # Ack malformed messages (2xx) — they can never succeed, so a non-2xx
        # would only make PubSub redeliver them forever. Log attribute keys
        # only; the values may carry sensitive data.
        logger.error(
            f"PubSub message missing required attribute 'destination_id'. "
            f"Attribute keys: {sorted(attributes.keys())}"
        )
        return {}
    # Push data rides in the message itself, so execution errors must propagate
    # (non-2xx) for PubSub to redeliver — acking a failed run would drop data.
    return await execute_action(
        integration_id=destination_id,
        data=json_payload,
        metadata=attributes
    )

app.include_router(
    actions.router, prefix="/v1/actions", tags=["actions"], responses={}
)
app.include_router(
    webhooks.router, prefix="/webhooks", tags=["webhooks"], responses={}
)
app.include_router(
    config_events.router, prefix="/config-events", tags=["configurations"], responses={}
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # The request body can carry draft credentials on the ephemeral path, so
    # neither the response nor the log gets it: log access and retention are
    # usually broader than access to the originating request. Keep only
    # loc/msg/type per error. On the pinned pydantic 1.x, `ctx` can carry
    # values from the offending input; `input` is dropped too so a pydantic 2
    # upgrade, which mirrors the value there, does not reopen the leak.
    safe_errors = [
        {k: v for k, v in err.items() if k not in ("input", "ctx")}
        for err in exc.errors()
    ]
    logger.debug("Failed handling body: %s", jsonable_encoder({"detail": safe_errors}))
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": safe_errors}),
    )
