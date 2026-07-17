import { UserManager, WebStorageStateStore } from 'oidc-client';
import { authConfig } from '../config/auth';

// Configuration for the Gundi API OpenID Connect
const config = {
  authority: authConfig.authority,
  client_id: authConfig.client_id,
  redirect_uri: authConfig.redirect_uri,
  response_type: authConfig.response_type,
  scope: authConfig.scope,
  post_logout_redirect_uri: authConfig.post_logout_redirect_uri,
  userStore: new WebStorageStateStore({ store: window.localStorage }),
  automaticSilentRenew: authConfig.automaticSilentRenew,
  silent_redirect_uri: authConfig.silent_redirect_uri,
  loadUserInfo: authConfig.loadUserInfo,
  filterProtocolClaims: authConfig.filterProtocolClaims,
  response_mode: authConfig.response_mode
};

class AuthService {
  constructor() {
    this.userManager = new UserManager(config);
    this.user = null;
    this.isAuthenticated = false;
    
    // Set up event handlers
    this.userManager.events.addUserLoaded((user) => {
      this.user = user;
      this.isAuthenticated = true;
    });
    
    this.userManager.events.addUserUnloaded(() => {
      this.user = null;
      this.isAuthenticated = false;
    });
    
    this.userManager.events.addAccessTokenExpiring(() => {
      console.log('Access token expiring');
    });
    
    this.userManager.events.addAccessTokenExpired(() => {
      console.log('Access token expired');
      this.signinSilent();
    });
  }

  async initialize() {
    try {
      const user = await this.userManager.getUser();
      if (user && !user.expired) {
        this.user = user;
        this.isAuthenticated = true;
      } else {
        this.user = null;
        this.isAuthenticated = false;
      }
    } catch (error) {
      console.error('Error initializing auth service:', error);
      this.user = null;
      this.isAuthenticated = false;
    }
  }

  async signin() {
    try {
      await this.userManager.signinRedirect();
    } catch (error) {
      console.error('Error during signin:', error);
      throw error;
    }
  }

  async signinRedirectCallback() {
    try {
      const user = await this.userManager.signinRedirectCallback();
      this.user = user;
      this.isAuthenticated = true;
      return user;
    } catch (error) {
      console.error('Error during signin callback:', error);
      throw error;
    }
  }

  async signinSilent() {
    try {
      const user = await this.userManager.signinSilent();
      this.user = user;
      this.isAuthenticated = true;
      return user;
    } catch (error) {
      console.error('Error during silent signin:', error);
      // If silent signin fails, user needs to re-authenticate
      this.user = null;
      this.isAuthenticated = false;
      throw error;
    }
  }

  async signinSilentCallback() {
    try {
      await this.userManager.signinSilentCallback();
    } catch (error) {
      console.error('Error during silent signin callback:', error);
      throw error;
    }
  }

  async signout() {
    try {
      await this.userManager.signoutRedirect();
    } catch (error) {
      console.error('Error during signout:', error);
      throw error;
    }
  }

  async getAccessToken() {
    if (this.user && !this.user.expired) {
      return this.user.access_token;
    }
    return null;
  }

  async getApiHeaders() {
    const token = await this.getAccessToken();
    if (token) {
      return {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      };
    }
    return {
      'Content-Type': 'application/json'
    };
  }

  getUser() {
    return this.user;
  }

  isUserAuthenticated() {
    return this.isAuthenticated && this.user && !this.user.expired;
  }
}

// Create a singleton instance
const authService = new AuthService();

export default authService;
