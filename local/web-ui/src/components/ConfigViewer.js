import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Alert,
  CircularProgress,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Chip,
  Divider,
  TextField,
  InputAdornment,
  FormControl,
  InputLabel,
  Select,
  MenuItem
} from '@mui/material';
import {
  Refresh as RefreshIcon,
  Search as SearchIcon,
  Settings as SettingsIcon,
  Visibility as VisibilityIcon,
  ContentCopy as CopyIcon,
  CheckCircle as CheckCircleIcon,
  RadioButtonUnchecked as RadioButtonUncheckedIcon
} from '@mui/icons-material';
import { useAuth } from '../contexts/AuthContext';
import { useConnection } from '../contexts/ConnectionContext';
import { authConfig } from '../config/auth';
import axios from 'axios';

const ConfigViewer = () => {
  const { getApiHeaders, isAuthenticated } = useAuth();
  const { selectedConnection, selectConnection } = useConnection();
  const [configurations, setConfigurations] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedConfig, setSelectedConfig] = useState(null);
  const [filterType, setFilterType] = useState('');
  const [availableTypes, setAvailableTypes] = useState([]);
  const [loadingTypes, setLoadingTypes] = useState(false);
  const [copiedId, setCopiedId] = useState(null);

  const fetchConfigurations = async (providerType = null) => {
    if (!isAuthenticated) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      
      const headers = await getApiHeaders();
      let url = `${authConfig.apiBaseUrl}/connections`;
      
      // Add provider_type query parameter if specified
      if (providerType) {
        url += `?provider_type=${encodeURIComponent(providerType)}`;
        console.log(`Fetching connections with provider_type: ${providerType}`);
      }
      
      const response = await axios.get(url, {
        headers
      });
      
      const connections = response.data.results || response.data || [];
      console.log('Fetched connections:', connections);
      if (connections.length > 0) {
        console.log('Sample connection structure:', connections[0]);
      }
      setConfigurations(connections);
    } catch (err) {
      console.error('Error fetching configurations:', err);
      setError(err.response?.data?.detail || err.message || 'Failed to fetch configurations');
    } finally {
      setLoading(false);
    }
  };

  const fetchIntegrationTypes = async () => {
    if (!isAuthenticated) {
      return;
    }

    try {
      setLoadingTypes(true);
      const headers = await getApiHeaders();
      let allTypes = [];
      let nextUrl = `${authConfig.apiBaseUrl}/integrations/types/`;
      
      // Fetch all pages of integration types
      while (nextUrl) {
        const response = await axios.get(nextUrl, { headers });
        const data = response.data;
        
        // Add current page results to our collection
        if (data.results) {
          allTypes = [...allTypes, ...data.results];
        } else if (Array.isArray(data)) {
          allTypes = [...allTypes, ...data];
        }
        
        // Check if there's a next page
        nextUrl = data.next || null;
      }
      
      console.log(`Fetched ${allTypes.length} integration types`);
      if (allTypes.length > 0) {
        console.log('Sample integration type structure:', allTypes[0]);
      }
      setAvailableTypes(allTypes);
    } catch (err) {
      console.error('Error fetching integration types:', err);
      // Don't set error for types, just log it
    } finally {
      setLoadingTypes(false);
    }
  };

  useEffect(() => {
    if (isAuthenticated) {
      fetchConfigurations();
      fetchIntegrationTypes();
    }
  }, [isAuthenticated]);

  // Apply only client-side search filtering since type filtering is now server-side
  const filteredConfigurations = configurations.filter(config => {
    const matchesSearch = config.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      config.id?.toString().includes(searchTerm) ||
      config.integration_type?.toLowerCase().includes(searchTerm.toLowerCase());
    
    return matchesSearch;
  });

  const handleViewConfig = (config) => {
    setSelectedConfig(config);
  };

  const handleCloseDetails = () => {
    setSelectedConfig(null);
  };

  const handleFilterChange = (event) => {
    const selectedValue = event.target.value;
    setFilterType(selectedValue);
    
    // Find the selected integration type to get its value
    const selectedType = availableTypes.find(type => 
      (type.id || type.name || type) === selectedValue
    );
    
    // Get the provider_type value from the selected integration type
    const providerType = selectedType?.value || null;
    
    // Fetch connections with the provider_type filter
    fetchConfigurations(providerType);
  };

  const clearFilters = () => {
    setSearchTerm('');
    setFilterType('');
    // Refetch all connections without any filter
    fetchConfigurations();
  };

  const copyToClipboard = async (id) => {
    try {
      await navigator.clipboard.writeText(id);
      setCopiedId(id);
      // Clear the copied state after 2 seconds
      setTimeout(() => setCopiedId(null), 2000);
    } catch (err) {
      console.error('Failed to copy to clipboard:', err);
      // Fallback for older browsers
      const textArea = document.createElement('textarea');
      textArea.value = id;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      setCopiedId(id);
      setTimeout(() => setCopiedId(null), 2000);
    }
  };

  const refreshData = async () => {
    // Find the current selected type to get its provider_type value
    const selectedType = availableTypes.find(type => 
      (type.id || type.name || type) === filterType
    );
    const providerType = selectedType?.value || null;
    
    await Promise.all([
      fetchConfigurations(providerType),
      fetchIntegrationTypes()
    ]);
  };

  if (!isAuthenticated) {
    return (
      <Alert severity="warning">
        Please log in to view configurations from the Gundi API.
      </Alert>
    );
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1">
          Gundi API Connections
        </Typography>
        <Button
          variant="contained"
          startIcon={loading ? <CircularProgress size={20} /> : <RefreshIcon />}
          onClick={refreshData}
          disabled={loading}
        >
          {loading ? 'Loading...' : 'Refresh'}
        </Button>
      </Box>

      {selectedConnection && (
        <Alert 
          severity="info" 
          sx={{ mb: 3 }}
          action={
            <Button 
              color="inherit" 
              size="small" 
              onClick={() => selectConnection(null)}
            >
              Clear Selection
            </Button>
          }
        >
          <Typography variant="body2">
            <strong>Selected Connection:</strong> {selectedConnection.provider?.name || selectedConnection.name || `Connection ${selectedConnection.id}`} 
            <Chip 
              label={selectedConnection.provider?.type?.name || 'Unknown Type'} 
              size="small" 
              sx={{ ml: 1 }} 
            />
            <Typography component="span" variant="caption" sx={{ ml: 1, opacity: 0.7 }}>
              (ID: {selectedConnection.id})
            </Typography>
          </Typography>
        </Alert>
      )}

      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', flexWrap: 'wrap' }}>
            <TextField
              placeholder="Search connections..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon />
                  </InputAdornment>
                ),
              }}
              sx={{ minWidth: 250, flexGrow: 1 }}
            />
            
            <FormControl sx={{ minWidth: 200 }}>
              <InputLabel>Filter by Type</InputLabel>
              <Select
                value={filterType}
                label="Filter by Type"
                onChange={handleFilterChange}
                disabled={loadingTypes}
              >
                <MenuItem value="">
                  <em>All Types ({availableTypes.length})</em>
                </MenuItem>
                {availableTypes.map((type) => (
                  <MenuItem key={type.id || type.name || type} value={type.id || type.name || type}>
                    {type.name || type.id || type}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            
            {(searchTerm || filterType) && (
              <Button
                variant="outlined"
                onClick={clearFilters}
                size="small"
              >
                Clear Filters
              </Button>
            )}
          </Box>
        </CardContent>
      </Card>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {loading && configurations.length === 0 ? (
        <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
          <CircularProgress />
        </Box>
      ) : (
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Connections ({filteredConfigurations.length})
              {filterType && (
                <Chip
                  label={`Type: ${availableTypes.find(type => (type.id || type.name || type) === filterType)?.name || filterType}`}
                  size="small"
                  sx={{ ml: 2 }}
                  onDelete={() => {
                    setFilterType('');
                    fetchConfigurations(); // Refetch all connections
                  }}
                />
              )}
            </Typography>
            
            {filteredConfigurations.length === 0 ? (
              <Typography color="text.secondary">
                {searchTerm || filterType 
                  ? 'No connections match your current filters.' 
                  : 'No connections found.'}
              </Typography>
            ) : (
              <List>
                {filteredConfigurations.map((config, index) => {

                  // Debug logging
                  console.log('Config:', config);
                  
                  return (
                    <React.Fragment key={config.id || index}>
                      <ListItem>
                        <ListItemText
                          primary={
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="h6" component="span">
                                {config.provider?.name || config.name || `Connection ${config.id}`}
                              </Typography>
                              <Chip
                                label={config.provider.type.name}
                                size="small"
                                color="primary"
                                variant="outlined"
                              />
                            </Box>
                          }
                          secondary={
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1 }}>
                              <Typography variant="body2" color="text.secondary">
                                ID: {config.id}
                              </Typography>
                              <IconButton
                                size="small"
                                onClick={() => copyToClipboard(config.id)}
                                title={copiedId === config.id ? "Copied!" : "Copy ID to clipboard"}
                                color={copiedId === config.id ? "success" : "default"}
                              >
                                <CopyIcon fontSize="small" />
                              </IconButton>
                              {copiedId === config.id && (
                                <Typography variant="caption" color="success.main">
                                  Copied!
                                </Typography>
                              )}
                            </Box>
                          }
                        />
                        <ListItemSecondaryAction>
                          <Box sx={{ display: 'flex', gap: 1 }}>
                            <IconButton
                              onClick={() => selectConnection(config)}
                              title={selectedConnection?.id === config.id ? "Selected connection" : "Select this connection"}
                              color={selectedConnection?.id === config.id ? "primary" : "default"}
                            >
                              {selectedConnection?.id === config.id ? (
                                <CheckCircleIcon />
                              ) : (
                                <RadioButtonUncheckedIcon />
                              )}
                            </IconButton>
                            <IconButton
                              onClick={() => handleViewConfig(config)}
                              title="View details"
                            >
                              <VisibilityIcon />
                            </IconButton>
                          </Box>
                        </ListItemSecondaryAction>
                      </ListItem>
                      {index < filteredConfigurations.length - 1 && <Divider />}
                    </React.Fragment>
                  );
                })}
              </List>
            )}
          </CardContent>
        </Card>
      )}

      {/* Configuration Details Modal/Dialog */}
      {selectedConfig && (
        <Card sx={{ mt: 3 }}>
          <CardContent>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
              <Typography variant="h6">
                Connection Details
              </Typography>
              <Button onClick={handleCloseDetails}>
                Close
              </Button>
            </Box>
            <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
              <pre style={{ 
                backgroundColor: '#f5f5f5', 
                padding: '16px', 
                borderRadius: '4px',
                fontSize: '12px',
                overflow: 'auto'
              }}>
                {JSON.stringify(selectedConfig, null, 2)}
              </pre>
            </Box>
          </CardContent>
        </Card>
      )}
    </Box>
  );
};

export default ConfigViewer;
