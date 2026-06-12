# Authentication Setup

This web UI now includes OpenID Connect authentication to access the Gundi API. Follow these steps to configure authentication.

## Prerequisites

1. Access to the Gundi API OIDC provider
2. Ability to register a new OIDC client
3. Knowledge of the OIDC authority URL

## Configuration Steps

### 1. Update Authentication Configuration

Edit `src/config/auth.js` and update the following values:

```javascript
export const authConfig = {
  // Update this to your actual OIDC authority URL
  authority: 'https://api.stage.gundiservice.org',
  
  // Register this client ID in your OIDC provider
  client_id: 'gundi-integration-ui',
  
  // These URLs should be registered in your OIDC provider
  redirect_uri: `${window.location.origin}/callback`,
  post_logout_redirect_uri: `${window.location.origin}/`,
  silent_redirect_uri: `${window.location.origin}/silent-callback`,
  
  // API base URL for making authenticated requests
  apiBaseUrl: 'https://api.stage.gundiservice.org/v2'
};
```

### 2. Register OIDC Client

In your OIDC provider (likely part of the Gundi API), register a new client with:

- **Client ID**: `gundi-integration-ui` (or update the config to match)
- **Client Type**: Public client (SPA - Single Page Application)
- **Redirect URIs**:
  - `http://localhost:3000/callback` (for local development)
  - `https://your-domain.com/callback` (for production)
- **Post Logout Redirect URIs**:
  - `http://localhost:3000/` (for local development)
  - `https://your-domain.com/` (for production)
- **Allowed Scopes**: `openid`, `profile`, `email`
- **Response Types**: `code`
- **Grant Types**: `authorization_code`, `refresh_token`

### 3. Environment-Specific Configuration

For different environments, you may want to use environment variables. Update `src/config/auth.js`:

```javascript
export const authConfig = {
  authority: process.env.REACT_APP_OIDC_AUTHORITY || 'https://api.stage.gundiservice.org',
  client_id: process.env.REACT_APP_OIDC_CLIENT_ID || 'cdip-oauth2',
  apiBaseUrl: process.env.REACT_APP_API_BASE_URL || 'https://api.stage.gundiservice.org/v2',
  // ... other config
};
```

Then create environment files:
- `.env.local` for local development
- `.env.production` for production

### 4. Test Authentication

1. Start the web UI: `npm start` or `docker compose up`
2. Navigate to `http://localhost:3000`
3. You should be redirected to the login page
4. Click "Sign in with Gundi" to test the OIDC flow
5. After successful authentication, you should see the main interface
6. Navigate to "Configurations" to test API access

## Features

### Authentication Flow
- **Login**: Redirects to OIDC provider for authentication
- **Silent Renewal**: Automatically renews tokens in the background
- **Logout**: Properly signs out from both the app and OIDC provider
- **Token Management**: Handles access tokens for API requests

### Protected Routes
- All main application routes require authentication
- Unauthenticated users are redirected to the login page
- Loading states during authentication checks

### API Integration
- Automatic token inclusion in API requests
- Error handling for expired tokens
- Configuration viewer for Gundi API data

## Troubleshooting

### Common Issues

1. **"Invalid redirect URI" error**
   - Ensure the redirect URIs in your OIDC client match exactly
   - Check for trailing slashes and protocol (http vs https)

2. **"Client not found" error**
   - Verify the client_id in your configuration
   - Ensure the client is properly registered in the OIDC provider

3. **CORS errors**
   - The OIDC provider needs to allow requests from your domain
   - Check the OIDC provider's CORS configuration

4. **Token expiration issues**
   - Ensure silent renewal is properly configured
   - Check that the silent redirect URI is registered

### Debug Mode

Enable debug logging by adding to your browser's console:
```javascript
localStorage.setItem('oidc.debug', 'true');
```

### Manual Token Inspection

You can inspect stored tokens in the browser's developer tools:
```javascript
// In browser console
JSON.parse(localStorage.getItem('oidc.user:https://api.stage.gundiservice.org:gundi-integration-ui'))
```

## Security Considerations

1. **Client ID**: The client ID is public and can be exposed in the frontend
2. **Token Storage**: Tokens are stored in localStorage (consider httpOnly cookies for production)
3. **HTTPS**: Always use HTTPS in production
4. **Token Expiration**: Tokens are automatically renewed, but users will need to re-authenticate if renewal fails

## API Usage

Once authenticated, the app can make requests to the Gundi API:

```javascript
import { useAuth } from '../contexts/AuthContext';

const MyComponent = () => {
  const { getApiHeaders } = useAuth();
  
  const fetchData = async () => {
    const headers = await getApiHeaders();
    const response = await axios.get('https://api.stage.gundiservice.org/v2/configurations/', {
      headers
    });
    return response.data;
  };
};
```

The `getApiHeaders()` function automatically includes the Bearer token for authenticated requests.
