# Running an Action Runner Locally

You can run this Action Runner locally using Docker Compose. This will allow you to develop the code and debug it locally while still taking advantage of Gundi Core services.

## Requirements

- Python 3.10
- Docker
- Docker Compose

## Steps

Inside the `local` directory, do these things:

**Set your Environment**

Create a copy of `.env.local.example` and name it `.env.local`

Edit the `.env.local` file and set the `KEYCLOAK_CLIENT_SECRET` to a secret from the stage environment (Ask the Gundi Team)

**Build and Run**

In a Terminal, run `docker compose up --build`

> [!NOTE]
>
> The Be sure to compile a `requirements.txt` file before running the docker compose command, or you might miss some important dependencies.


Once these steps are complete you can visit http://localhost:8080/docs to see your Action Runner's browsable API.

## Notes

- This example uses configuration from https://stage.gundiservice.org.




