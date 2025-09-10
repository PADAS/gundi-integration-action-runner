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

## Authentication

The web UI now includes OpenID Connect authentication to access the Gundi API. By default, the app will redirect to a login page since authentication is required.

### Configuration

To configure authentication, edit `local/web-ui/src/config/auth.js` and update:

- `authority`: Your OIDC provider URL
- `client_id`: Your registered OIDC client ID
- `apiBaseUrl`: The Gundi API base URL

### Features

- **Login/Logout**: Secure authentication with the Gundi API
- **Protected Routes**: All main features require authentication
- **Token Management**: Automatic token renewal and management
- **API Integration**: Access to Gundi API connections and data
- **User Profile**: Display user information in the header

For detailed setup instructions, see `local/web-ui/AUTHENTICATION.md`.

## Web UI

The local deployment now includes a React-based web UI that provides a user-friendly interface for:
- Viewing all available actions
 this - Executing actions with custom parameters
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




