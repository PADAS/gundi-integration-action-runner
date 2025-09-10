import React, { createContext, useContext, useEffect, useState } from 'react';
import authService from '../services/authService';

const AuthContext = createContext();

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const initializeAuth = async () => {
      try {
        setIsLoading(true);
        setError(null);
        await authService.initialize();
        setUser(authService.getUser());
        setIsAuthenticated(authService.isUserAuthenticated());
      } catch (err) {
        console.error('Error initializing authentication:', err);
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    initializeAuth();
  }, []);

  const signin = async () => {
    try {
      setError(null);
      await authService.signin();
    } catch (err) {
      console.error('Error during signin:', err);
      setError(err.message);
      throw err;
    }
  };

  const signout = async () => {
    try {
      setError(null);
      await authService.signout();
      setUser(null);
      setIsAuthenticated(false);
    } catch (err) {
      console.error('Error during signout:', err);
      setError(err.message);
      throw err;
    }
  };

  const handleSigninCallback = async () => {
    try {
      setError(null);
      const user = await authService.signinRedirectCallback();
      setUser(user);
      setIsAuthenticated(true);
      return user;
    } catch (err) {
      console.error('Error during signin callback:', err);
      setError(err.message);
      throw err;
    }
  };

  const handleSilentCallback = async () => {
    try {
      setError(null);
      await authService.signinSilentCallback();
    } catch (err) {
      console.error('Error during silent callback:', err);
      setError(err.message);
      throw err;
    }
  };

  const getAccessToken = async () => {
    return await authService.getAccessToken();
  };

  const getApiHeaders = async () => {
    return await authService.getApiHeaders();
  };

  const value = {
    user,
    isAuthenticated,
    isLoading,
    error,
    signin,
    signout,
    handleSigninCallback,
    handleSilentCallback,
    getAccessToken,
    getApiHeaders
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};
