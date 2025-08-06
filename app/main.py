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


# For running behind a proxy, we'll want to configure the root path for OpenAPI browser.
root_path = os.environ.get("ROOT_PATH", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup Hook
    if settings.REGISTER_ON_START:
        await register_integration_in_gundi(gundi_client=_portal)
        # ToDo: set env var to false in GCP after registration
    yield
    # Shotdown Hook
    await _portal.close()


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
    if settings.PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND:
        background_tasks.add_task(
            execute_action,
            integration_id=json_payload.get("integration_id"),
            action_id=json_payload.get("action_id"),
            config_overrides=json_payload.get("config_overrides"),
        )
    else:
        await execute_action(
            integration_id=json_payload.get("integration_id"),
            action_id=json_payload.get("action_id"),
            config_overrides=json_payload.get("config_overrides"),
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
    await execute_action(
        integration_id=attributes.get("destination_id"),
        data=json_payload,
    )
    return {}

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

    logger.debug(
        "Failed handling body: %s",
        jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )
