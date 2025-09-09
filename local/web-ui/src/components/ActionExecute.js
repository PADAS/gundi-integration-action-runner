import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  Box,
  Alert,
  CircularProgress,
  FormControlLabel,
  Switch,
  Paper,
  Divider
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import axios from 'axios';
import DynamicForm from './DynamicForm';

const ActionExecute = () => {
  const { actionId } = useParams();
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    integration_id: '',
    action_id: actionId,
    run_in_background: false,
    config_overrides: {}
  });
  const [showDynamicForm, setShowDynamicForm] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleInputChange = (field) => (event) => {
    setFormData({
      ...formData,
      [field]: event.target.value
    });
  };

  const handleSwitchChange = (field) => (event) => {
    setFormData({
      ...formData,
      [field]: event.target.checked
    });
  };

  const handleConfigOverrideChange = (key) => (event) => {
    setFormData({
      ...formData,
      config_overrides: {
        ...formData.config_overrides,
        [key]: event.target.value
      }
    });
  };

  const handleSubmit = async (event) => {
    event.preventDefault();
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      // Use the FastAPI service URL that the browser can access
      const response = await axios.post('http://localhost:8080/v1/actions/execute', formData);
      setResult(response.data);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to execute action');
      console.error('Error executing action:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleDynamicFormSubmit = async (formData) => {
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const requestData = {
        integration_id: formData.integration_id,
        action_id: actionId,
        run_in_background: formData.run_in_background,
        config_overrides: formData.config_overrides
      };
      
      const response = await axios.post('http://localhost:8080/v1/actions/execute', requestData);
      setResult(response.data);
      setShowDynamicForm(false);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to execute action');
      console.error('Error executing action:', err);
    } finally {
      setLoading(false);
    }
  };

  const addConfigOverride = () => {
    const key = prompt('Enter config key:');
    if (key) {
      setFormData({
        ...formData,
        config_overrides: {
          ...formData.config_overrides,
          [key]: ''
        }
      });
    }
  };

  const removeConfigOverride = (key) => {
    const newConfigOverrides = { ...formData.config_overrides };
    delete newConfigOverrides[key];
    setFormData({
      ...formData,
      config_overrides: newConfigOverrides
    });
  };

  return (
    <Box>
      <Button
        startIcon={<ArrowBackIcon />}
        onClick={() => navigate('/')}
        sx={{ mb: 2 }}
      >
        Back to Actions
      </Button>

      <Typography variant="h4" component="h1" gutterBottom>
        Execute Action: {actionId}
      </Typography>

      {!showDynamicForm ? (
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Basic Configuration
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Choose how to configure this action
            </Typography>

            <TextField
              fullWidth
              label="Integration ID"
              value={formData.integration_id}
              onChange={handleInputChange('integration_id')}
              required
              sx={{ mb: 2 }}
              helperText="Enter the integration ID for this action"
            />

            <FormControlLabel
              control={
                <Switch
                  checked={formData.run_in_background}
                  onChange={handleSwitchChange('run_in_background')}
                />
              }
              label="Run in Background"
              sx={{ mb: 3 }}
            />

            <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
              <Button
                variant="contained"
                onClick={() => setShowDynamicForm(true)}
              >
                Use Dynamic Form
              </Button>
              <Button
                variant="outlined"
                onClick={() => setShowDynamicForm(false)}
              >
                Use Manual Configuration
              </Button>
            </Box>

            <Divider sx={{ my: 2 }} />

            <Typography variant="h6" gutterBottom>
              Manual Configuration Overrides
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Add custom configuration parameters for this action
            </Typography>

            {Object.entries(formData.config_overrides).map(([key, value]) => (
              <Box key={key} sx={{ display: 'flex', gap: 1, mb: 1 }}>
                <TextField
                  label="Key"
                  value={key}
                  disabled
                  sx={{ flex: 1 }}
                />
                <TextField
                  label="Value"
                  value={value}
                  onChange={handleConfigOverrideChange(key)}
                  sx={{ flex: 2 }}
                />
                <Button
                  variant="outlined"
                  color="error"
                  onClick={() => removeConfigOverride(key)}
                  sx={{ minWidth: 'auto' }}
                >
                  Remove
                </Button>
              </Box>
            ))}

            <Button
              variant="outlined"
              onClick={addConfigOverride}
              sx={{ mb: 3 }}
            >
              Add Config Override
            </Button>

            <Box sx={{ display: 'flex', gap: 2 }}>
              <Button
                onClick={handleSubmit}
                variant="contained"
                startIcon={loading ? <CircularProgress size={20} /> : <PlayArrowIcon />}
                disabled={loading || !formData.integration_id}
                size="large"
              >
                {loading ? 'Executing...' : 'Execute Action'}
              </Button>
            </Box>
          </CardContent>
        </Card>
      ) : (
        <Box sx={{ mb: 3 }}>
          <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
            <Button
              variant="outlined"
              onClick={() => setShowDynamicForm(false)}
            >
              Back to Manual Configuration
            </Button>
          </Box>
          
          <DynamicForm
            actionId={actionId}
            onSubmit={handleDynamicFormSubmit}
            onCancel={() => setShowDynamicForm(false)}
            initialIntegrationId={formData.integration_id}
            initialRunInBackground={formData.run_in_background}
          />
        </Box>
      )}

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {result && (
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>
            Execution Result
          </Typography>
          <pre style={{ 
            backgroundColor: '#f5f5f5', 
            padding: '16px', 
            borderRadius: '4px',
            overflow: 'auto',
            maxHeight: '400px'
          }}>
            {JSON.stringify(result, null, 2)}
          </pre>
        </Paper>
      )}
    </Box>
  );
};

export default ActionExecute;
