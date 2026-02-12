# Authentication Troubleshooting Guide

## CORS Error: "Origin null is not allowed by Access-Control-Allow-Origin"

This error occurs when the OIDC provider (Keycloak) doesn't allow requests from your application's origin. Here's how to fix it:

### 1. Check OIDC Client Configuration in Keycloak

You need to configure the `cdip-oauth2` client in Keycloak to allow your application:

1. **Log into Keycloak Admin Console**:
   - Go to `https://cdip-auth.pamdas.org/auth/admin/`
   - Log in with admin credentials

2. **Navigate to the Client**:
   - Go to "Clients" in the left sidebar
   - Find and click on `cdip-oauth2`

3. **Configure Client Settings**:
   - **Client Protocol**: `openid-connect`
   - **Access Type**: `public` (for SPA applications)
   - **Standard Flow Enabled**: `ON`
   - **Implicit Flow Enabled**: `OFF`
   - **Direct Access Grants Enabled**: `OFF`
   - **Service Accounts Enabled**: `OFF`

4. **Add Valid Redirect URIs**:
   - In the "Valid Redirect URIs" field, add:
     ```
     http://localhost:3000/callback
     http://localhost:3000/silent-callback
     ```
   - In the "Valid Post Logout Redirect URIs" field, add:
     ```
     http://localhost:3000/
     ```

5. **Configure Web Origins**:
   - In the "Web Origins" field, add:
     ```
     http://localhost:3000
     +
     ```

6. **Save the Configuration**

### 2. Verify OIDC Discovery Endpoint

Test if the OIDC discovery endpoint is accessible:

```bash
curl "https://cdip-auth.pamdas.org/auth/realms/cdip-dev/.well-known/openid_configuration"
```

This should return a JSON response with OIDC configuration.

### 3. Test Client Configuration

You can test the client configuration by visiting the authorization endpoint directly:

```
https://cdip-auth.pamdas.org/auth/realms/cdip-dev/protocol/openid-connect/auth?client_id=cdip-oauth2&redirect_uri=http://localhost:3000/callback&response_type=code&scope=openid%20profile%20email
```

### 4. Browser Developer Tools Debugging

1. **Open Browser Developer Tools** (F12)
2. **Go to Console tab** - Look for OIDC-related error messages
3. **Go to Network tab** - Check for failed requests to the OIDC provider
4. **Check Application tab** - Look at localStorage for OIDC tokens

### 5. Common Issues and Solutions

#### Issue: "Invalid redirect URI"
**Solution**: Ensure the redirect URI in Keycloak exactly matches `http://localhost:3000/callback`

#### Issue: "Client not found"
**Solution**: Verify the client ID `cdip-oauth2` exists in Keycloak

#### Issue: "CORS error"
**Solution**: Add `http://localhost:3000` to the "Web Origins" in Keycloak client settings

#### Issue: "Access denied"
**Solution**: Check that the client is configured as "public" and has the correct flow enabled

### 6. Alternative: Use Environment Variables

If you need different configurations for different environments, you can use environment variables:

Create `.env.local` in the web-ui directory:

```env
REACT_APP_OIDC_AUTHORITY=https://cdip-auth.pamdas.org/auth/realms/cdip-dev
REACT_APP_OIDC_CLIENT_ID=cdip-oauth2
REACT_APP_API_BASE_URL=https://api.stage.gundiservice.org/v2
```

Then update `src/config/auth.js`:

```javascript
export const authConfig = {
  authority: process.env.REACT_APP_OIDC_AUTHORITY || 'https://cdip-auth.pamdas.org/auth/realms/cdip-dev',
  client_id: process.env.REACT_APP_OIDC_CLIENT_ID || 'cdip-oauth2',
  apiBaseUrl: process.env.REACT_APP_API_BASE_URL || 'https://api.stage.gundiservice.org/v2',
  // ... rest of config
};
```

### 7. Testing the Fix

After making changes in Keycloak:

1. **Clear browser cache and localStorage**
2. **Restart the web UI**: `docker compose restart web-ui`
3. **Visit** `http://localhost:3000`
4. **Check browser console** for any remaining errors
5. **Try the login flow** again

### 8. Getting Help

If you continue to have issues:

1. **Check Keycloak logs** for any server-side errors
2. **Verify network connectivity** to the OIDC provider
3. **Test with a simple OIDC client** to isolate the issue
4. **Contact the Keycloak administrator** to verify client configuration

### 9. Debug Mode

The application includes debug logging. To enable it:

1. Open browser console
2. Run: `localStorage.setItem('oidc.debug', 'true')`
3. Refresh the page
4. Check console for detailed OIDC logs
