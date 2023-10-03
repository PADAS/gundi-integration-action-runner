#!/bin/bash

SERVER_IP=${SERVER_IP:-0.0.0.0}
API_PORT=${API_PORT:-9999}

GUNICORN_CMD_ARGS=${GUNICORN_CMD_ARGS:-"-w 2 -t 4 --timeout 60 --max-requests 500000 --max-requests-jitter 500"}
export GUNICORN_CMD_ARGS

echo "GUNICORN_CMD_ARGS=${GUNICORN_CMD_ARGS}"
# This is the essential bit we'll use to run the API service in production.
gunicorn -k uvicorn.workers.UvicornWorker app.main:app -b ${SERVER_IP}:${API_PORT}
