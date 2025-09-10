import React, { useState, useEffect } from 'react';
import {
  TextField,
  Button,
  Box,
  Typography,
  FormControl,
  FormHelperText,
  Switch,
  FormControlLabel,
  Select,
  MenuItem,
  InputLabel,
  Alert,
  CircularProgress,
  Paper,
  Divider
} from '@mui/material';
import axios from 'axios';
import { useConnection } from '../contexts/ConnectionContext';

const DynamicForm = ({ actionId, onSubmit, onCancel, initialIntegrationId = '', initialRunInBackground = false }) => {
  const { selectedConnection } = useConnection();
  const [schema, setSchema] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [formData, setFormData] = useState({
    integration_id: initialIntegrationId,
    run_in_background: initialRunInBackground,
    config_overrides: {}
  });
  const [validationErrors, setValidationErrors] = useState({});

  useEffect(() => {
    fetchSchema();
  }, [actionId]);

  // Update integration_id when selected connection changes
  useEffect(() => {
    if (selectedConnection?.id) {
      setFormData(prev => ({
        ...prev,
        integration_id: selectedConnection.id
      }));
    }
  }, [selectedConnection]);

  const fetchSchema = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`http://localhost:8080/v1/actions/${actionId}/schema`);
      setSchema(response.data);
      
      // Initialize form data with default values
      const initialData = {
        integration_id: formData.integration_id,
        run_in_background: formData.run_in_background,
        config_overrides: {}
      };
      if (response.data.config_schema.properties) {
        Object.keys(response.data.config_schema.properties).forEach(field => {
          const fieldSchema = response.data.config_schema.properties[field];
          if (fieldSchema.default !== undefined) {
            initialData.config_overrides[field] = fieldSchema.default;
          } else if (fieldSchema.type === 'boolean') {
            initialData.config_overrides[field] = false;
          } else if (fieldSchema.type === 'array') {
            initialData.config_overrides[field] = [];
          } else {
            initialData.config_overrides[field] = '';
          }
        });
      }
      setFormData(initialData);
    } catch (err) {
      setError('Failed to load form schema');
      console.error('Error fetching schema:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleInputChange = (field, value) => {
    setFormData(prev => ({
      ...prev,
      [field]: value
    }));
    
    // Clear validation error for this field
    if (validationErrors[field]) {
      setValidationErrors(prev => ({
        ...prev,
        [field]: null
      }));
    }
  };

  const handleConfigOverrideChange = (field, value) => {
    setFormData(prev => ({
      ...prev,
      config_overrides: {
        ...prev.config_overrides,
        [field]: value
      }
    }));
    
    // Clear validation error for this field
    if (validationErrors[field]) {
      setValidationErrors(prev => ({
        ...prev,
        [field]: null
      }));
    }
  };

  const validateForm = () => {
    const errors = {};
    
    // Validate integration_id
    if (!formData.integration_id) {
      errors.integration_id = 'Integration ID is required';
    }
    
    // Validate required config fields
    if (schema?.config_schema?.required) {
      schema.config_schema.required.forEach(field => {
        const value = formData.config_overrides[field];
        if (value === undefined || value === null || value === '') {
          errors[field] = 'This field is required';
        }
      });
    }
    
    setValidationErrors(errors);
    return errors;
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    const errors = validateForm();
    
    if (Object.keys(errors).length === 0) {
      onSubmit(formData);
    }
  };

  const renderField = (fieldName, fieldSchema) => {
    const value = formData.config_overrides[fieldName] || '';
    const error = validationErrors[fieldName];
    const isRequired = schema?.config_schema?.required?.includes(fieldName);

    // Handle password fields
    if (fieldSchema.format === 'password') {
      return (
        <TextField
          key={fieldName}
          fullWidth
          type="password"
          label={fieldSchema.title || fieldName}
          value={value}
          onChange={(e) => handleConfigOverrideChange(fieldName, e.target.value)}
          required={isRequired}
          error={!!error}
          helperText={error || fieldSchema.description}
          sx={{ mb: 2 }}
        />
      );
    }

    // Handle boolean fields
    if (fieldSchema.type === 'boolean') {
      return (
        <FormControlLabel
          key={fieldName}
          control={
            <Switch
              checked={value}
              onChange={(e) => handleConfigOverrideChange(fieldName, e.target.checked)}
            />
          }
          label={fieldSchema.title || fieldName}
          sx={{ mb: 2 }}
        />
      );
    }

    // Handle enum fields
    if (fieldSchema.enum) {
      return (
        <FormControl key={fieldName} fullWidth sx={{ mb: 2 }}>
          <InputLabel>{fieldSchema.title || fieldName}</InputLabel>
          <Select
            value={value}
            label={fieldSchema.title || fieldName}
            onChange={(e) => handleConfigOverrideChange(fieldName, e.target.value)}
            error={!!error}
            required={isRequired}
          >
            {fieldSchema.enum.map((option) => (
              <MenuItem key={option} value={option}>
                {option}
              </MenuItem>
            ))}
          </Select>
          {error && <FormHelperText error>{error}</FormHelperText>}
          {fieldSchema.description && !error && (
            <FormHelperText>{fieldSchema.description}</FormHelperText>
          )}
        </FormControl>
      );
    }

    // Handle number fields
    if (fieldSchema.type === 'number' || fieldSchema.type === 'integer') {
      return (
        <TextField
          key={fieldName}
          fullWidth
          type="number"
          label={fieldSchema.title || fieldName}
          value={value}
          onChange={(e) => handleConfigOverrideChange(fieldName, parseFloat(e.target.value) || '')}
          required={isRequired}
          error={!!error}
          helperText={error || fieldSchema.description}
          sx={{ mb: 2 }}
        />
      );
    }

    // Default to text field
    return (
      <TextField
        key={fieldName}
        fullWidth
        label={fieldSchema.title || fieldName}
        value={value}
        onChange={(e) => handleConfigOverrideChange(fieldName, e.target.value)}
        required={isRequired}
        error={!!error}
        helperText={error || fieldSchema.description}
        sx={{ mb: 2 }}
      />
    );
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
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

  if (!schema) {
    return (
      <Alert severity="warning" sx={{ mb: 2 }}>
        No schema available for this action
      </Alert>
    );
  }

  return (
    <Paper sx={{ p: 3 }}>
      <Typography variant="h6" gutterBottom>
        {schema.description || `Configure ${actionId} Action`}
      </Typography>
      
      <form onSubmit={handleSubmit}>
        {/* Integration ID field */}
        <TextField
          fullWidth
          label="Integration ID"
          value={formData.integration_id}
          onChange={(e) => handleInputChange('integration_id', e.target.value)}
          required
          error={!!validationErrors.integration_id}
          helperText={validationErrors.integration_id || "Enter the integration ID for this action"}
          sx={{ mb: 2 }}
        />

        {/* Run in Background switch */}
        <FormControlLabel
          control={
            <Switch
              checked={formData.run_in_background}
              onChange={(e) => handleInputChange('run_in_background', e.target.checked)}
            />
          }
          label="Run in Background"
          sx={{ mb: 3 }}
        />

        <Divider sx={{ my: 2 }} />

        <Typography variant="h6" gutterBottom>
          Action Configuration
        </Typography>
        
        {schema.config_schema.properties && 
          Object.entries(schema.config_schema.properties).map(([fieldName, fieldSchema]) => 
            renderField(fieldName, fieldSchema)
          )
        }
        
        <Box sx={{ display: 'flex', gap: 2, mt: 3 }}>
          <Button
            type="submit"
            variant="contained"
            color="primary"
          >
            Execute Action
          </Button>
          {onCancel && (
            <Button
              variant="outlined"
              onClick={onCancel}
            >
              Cancel
            </Button>
          )}
        </Box>
      </form>
    </Paper>
  );
};

export default DynamicForm;
