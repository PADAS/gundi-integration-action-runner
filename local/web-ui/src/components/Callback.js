import React, { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  CircularProgress,
  Typography,
  Alert,
  Container
} from '@mui/material';
import { useAuth } from '../contexts/AuthContext';

const Callback = () => {
  const { handleSigninCallback } = useAuth();
  const navigate = useNavigate();
  const [error, setError] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const hasProcessed = useRef(false);

  useEffect(() => {
    // Prevent multiple executions
    if (hasProcessed.current) {
      return;
    }
    hasProcessed.current = true;

    const processCallback = async () => {
      try {
        setIsLoading(true);
        setError(null);
        await handleSigninCallback();
        // Redirect to home page after successful authentication
        navigate('/', { replace: true });
      } catch (err) {
        console.error('Callback processing failed:', err);
        setError(err.message || 'Authentication failed');
      } finally {
        setIsLoading(false);
      }
    };

    processCallback();
  }, [handleSigninCallback, navigate]);

  if (error) {
    return (
      <Container maxWidth="sm">
        <Box
          sx={{
            minHeight: '100vh',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center'
          }}
        >
          <Alert severity="error" sx={{ width: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Authentication Error
            </Typography>
            <Typography variant="body2">
              {error}
            </Typography>
          </Alert>
        </Box>
      </Container>
    );
  }

  return (
    <Container maxWidth="sm">
      <Box
        sx={{
          minHeight: '100vh',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center'
        }}
      >
        <CircularProgress size={60} sx={{ mb: 2 }} />
        <Typography variant="h6" gutterBottom>
          Completing sign in...
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Please wait while we complete your authentication
        </Typography>
      </Box>
    </Container>
  );
};

export default Callback;
