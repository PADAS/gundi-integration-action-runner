# Gundi Integration Web UI

This is a React-based web application that provides a friendly user interface for the Gundi Integration Actions Execution Service.

## Features

- **Actions List**: View all available actions from the FastAPI service
- **Action Execution**: Execute actions with custom parameters
- **Real-time Results**: See execution results in real-time
- **Modern UI**: Built with Material-UI for a clean, professional look

## Development

### Prerequisites

- Node.js 18 or higher
- npm or yarn

### Local Development

1. Install dependencies:
   ```bash
   npm install
   ```

2. Start the development server:
   ```bash
   npm start
   ```

3. The app will be available at `http://localhost:3000`

### Building for Production

```bash
npm run build
```

## Docker Deployment

The web UI is configured to run as part of the local Docker Compose setup. It will automatically:

- Build the React application
- Serve it on port 3000
- Connect to the FastAPI service running on port 8080

## API Integration

The web UI communicates with the FastAPI service through the following endpoints:

- `GET /v1/actions/` - List all available actions
- `POST /v1/actions/execute` - Execute an action with parameters

## Configuration

The web UI uses the following environment variables:

- `REACT_APP_API_URL` - URL of the FastAPI service (defaults to `http://localhost:8080`)

## Usage

1. Navigate to the web UI at `http://localhost:3000`
2. View the list of available actions
3. Click "Execute" on any action to configure and run it
4. Fill in the required parameters (Integration ID, etc.)
5. Add any configuration overrides if needed
6. Click "Execute Action" to run the action
7. View the results in the response section

## Troubleshooting

- If the web UI can't connect to the FastAPI service, ensure the FastAPI service is running and healthy
- Check the browser console for any JavaScript errors
- Verify that the proxy configuration in `package.json` is correct for your setup
