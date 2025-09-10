import React, { useEffect } from 'react';
import { useAuth } from '../contexts/AuthContext';

const SilentCallback = () => {
  const { handleSilentCallback } = useAuth();

  useEffect(() => {
    const processSilentCallback = async () => {
      try {
        await handleSilentCallback();
      } catch (err) {
        console.error('Silent callback processing failed:', err);
      }
    };

    processSilentCallback();
  }, [handleSilentCallback]);

  // Silent callback should not render anything visible
  return null;
};

export default SilentCallback;
