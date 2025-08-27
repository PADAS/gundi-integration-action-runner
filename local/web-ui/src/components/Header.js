import React from 'react';
import { AppBar, Toolbar, Typography, Button, Box } from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';

const Header = () => {
  return (
    <AppBar position="static">
      <Toolbar>
        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          Gundi Integration UI
        </Typography>
        <Box>
          <Button 
            color="inherit" 
            component={RouterLink} 
            to="/"
            startIcon={<PlayArrowIcon />}
          >
            Actions
          </Button>
        </Box>
      </Toolbar>
    </AppBar>
  );
};

export default Header;
