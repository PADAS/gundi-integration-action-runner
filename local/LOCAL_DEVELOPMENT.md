# Running this function locally

You can run this Action Runner locally using Docker Compose. This will allow you to develop the code and debug it locally while still taking advantage of Gundi Core services.

## Requirements

- Python 3.10
- Docker
- Docker Compose

## Steps

Inside the `local` directory, do these things:

1. Create a copy of `.env.local.example` and name it `.env.local`
2. Edit the `.env.local` file and set the `KEYCLOAK_CLIENT_SECRET` to a secret from the stage environment (Ask the Gundi Team)
3. In a Terminal, run `docker compose up --build`

Once these steps are complete you can visit http://localhost:8080/docs to see your function's API documentation.

## Notes

This example uses configuration from https://stage.gundiservice.org.



