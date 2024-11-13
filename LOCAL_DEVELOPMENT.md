# Running this function locally

## Requirements

- Python 3.10
- Docker
- Docker Compose

## Steps

1. Create a copy of `.env.local.example` and name it `.env.local`
2. Edit the `.env.local` file and set the `KEYCLOAK_CLIENT_SECRET` to a secret from the stage environment (Ask the Gundi Team)
3. Run `docker compose -f local/docker-compose.yml up --build`
4. Run `curl -X PUT http://localhost:8085/v1/projects/local-project/topics/integration-events` to create a pubsub topic.

Once these steps are complete you can visit http://localhost:8080/docs to see your function's API documentation.

## Notes

This example uses configuration from https://stage.gundiservice.org.



