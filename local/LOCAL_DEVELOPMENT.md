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


Once these steps are complete you can visit:
- http://localhost:8080/docs to see your Action Runner's browsable API
- **http://localhost:3000** to access the web UI for a friendly interface to execute actions

## Web UI

The local deployment now includes a React-based web UI that provides a user-friendly interface for:
- Viewing all available actions
- Executing actions with custom parameters
- Viewing execution results in real-time

The web UI is automatically built and served when you run `docker compose up --build`.

### Troubleshooting Web UI

If you see a blank page at http://localhost:3000:

1. **Check container status**: `docker compose ps`
2. **View web UI logs**: `docker compose logs web-ui`
3. **Verify FastAPI is running**: `curl http://localhost:8080/v1/actions/`
4. **Rebuild if needed**: `docker compose build web-ui && docker compose up -d web-ui`

## Notes

- This example uses configuration from https://stage.gundiservice.org.
- The web UI connects to the FastAPI service running on port 8080




