// OpenID Connect Configuration
// Update these values based on your Gundi API OIDC setup

export const authConfig = {
  // The OIDC authority URL - this should point to your Gundi API's OIDC endpoint
  authority: 'https://cdip-auth.pamdas.org/auth/realms/cdip-dev',
  
  // Client ID - this needs to be registered in your OIDC provider
  client_id: 'cdip-oauth2',
  
  // Redirect URIs - these should be registered in your OIDC provider
  redirect_uri: `${window.location.origin}/callback`,
  post_logout_redirect_uri: `${window.location.origin}/`,
  silent_redirect_uri: `${window.location.origin}/silent-callback`,
  
  // OIDC flow configuration
  response_type: 'code',
  scope: 'openid profile email',
  response_mode: 'query',
  
  // Token management
  automaticSilentRenew: true,
  loadUserInfo: true,
  filterProtocolClaims: true,
  
  // Storage
  userStore: 'localStorage', // Will be converted to WebStorageStateStore in authService
  
  // API Configuration
  apiBaseUrl: 'https://api.stage.gundiservice.org/v2',
  
  // Debug mode for troubleshooting
  debug: true
};

// Helper function to get the full OIDC configuration
export const getOidcConfig = () => {
  return {
    ...authConfig,
    // Convert string to actual WebStorageStateStore instance
    userStore: new (require('oidc-client').WebStorageStateStore)({ 
      store: window.localStorage 
    })
  };
};
