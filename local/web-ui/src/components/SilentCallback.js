import React, { useEffect, useRef } from 'react';
import { useAuth } from '../contexts/AuthContext';

const SilentCallback = () => {
  const { handleSilentCallback } = useAuth();
  const hasProcessed = useRef(false);

  useEffect(() => {
    // Prevent multiple executions
    if (hasProcessed.current) {
      return;
    }
    hasProcessed.current = true;

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
