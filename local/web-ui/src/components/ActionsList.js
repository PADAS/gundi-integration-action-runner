import React, { useState, useEffect } from 'react';
import {
  Card,
  CardContent,
  Typography,
  Grid,
  Button,
  CircularProgress,
  Alert,
  Box
} from '@mui/material';
import { useNavigate } from 'react-router-dom';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import axios from 'axios';

const ActionsList = () => {
  const [actions, setActions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  useEffect(() => {
    fetchActions();
  }, []);

  const fetchActions = async () => {
    try {
      setLoading(true);
      // Use the FastAPI service URL that the browser can access
      const response = await axios.get('http://localhost:8080/v1/actions/');
      setActions(response.data);
      setError(null);
    } catch (err) {
      setError('Failed to fetch actions. Please check if the FastAPI service is running.');
      console.error('Error fetching actions:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleExecuteAction = (actionId) => {
    navigate(`/execute/${actionId}`);
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ mb: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Available Actions
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
        Select an action to execute with custom parameters
      </Typography>
      
      <Grid container spacing={3}>
        {actions.map((action) => (
          <Grid item xs={12} sm={6} md={4} key={action}>
            <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <CardContent sx={{ flexGrow: 1 }}>
                <Typography variant="h6" component="h2" gutterBottom>
                  {action}
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  Execute the {action} action with custom configuration
                </Typography>
                <Box sx={{ mt: 'auto' }}>
                  <Button
                    variant="contained"
                    startIcon={<PlayArrowIcon />}
                    onClick={() => handleExecuteAction(action)}
                    fullWidth
                  >
                    Execute
                  </Button>
                </Box>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>
      
      {actions.length === 0 && (
        <Alert severity="info">
          No actions available. Please check the FastAPI service configuration.
        </Alert>
      )}
    </Box>
  );
};

export default ActionsList;
