#!/bin/sh

# Bootstraps the local Pub/Sub emulator for action-runner development.

# Wait until the emulator responds, creating the integration-events topic.
until curl -X PUT http://pubsub_emulator:8085/v1/projects/local-project/topics/integration-events; do sleep 2; done

# The topic the action runner publishes sub-actions to.
curl -X PUT http://pubsub_emulator:8085/v1/projects/local-project/topics/local-actions-topic

# Push subscription looping sub-action messages back into the runner service.
curl http://pubsub_emulator:8085/v1/projects/local-project/subscriptions/local-actions-subscription \
 --data '{"topic": "projects/local-project/topics/local-actions-topic", "pushConfig": {"pushEndpoint": "http://connector:8080/"}}' \
  -X PUT -H 'content-type: application/json'
