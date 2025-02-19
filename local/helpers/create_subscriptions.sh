#!/bin/sh

# This script is used as part of automating a local development environment, particlarly for testing
# sub-actions in an Action Runner.

# This first operation is meant to wait until the PubSub emulator is up and running.
until curl -X PUT http://pubsub_emulator:8085/v1/projects/local-project/topics/integration-events; do sleep 2; done

# This createst the `local-actions-topic` topic. It's where the action runner is going to publish sub-actions.
curl -X PUT http://pubsub_emulator:8085/v1/projects/local-project/topics/local-actions-topic

# ...and this is where the subscription is made, to push 'local-actions-topic' messages to the FastAPI server (the action runner service).
curl http://pubsub_emulator:8085/v1/projects/local-project/subscriptions/local-actions-subscription \
 --data '{"topic": "projects/local-project/topics/local-actions-topic", "pushConfig": {"pushEndpoint": "http://fastapi:8080/"}}' \
  -X PUT -H 'content-type: application/json'