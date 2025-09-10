import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { Container, Box } from '@mui/material';
import { AuthProvider } from './contexts/AuthContext';
import Header from './components/Header';
import Login from './components/Login';
import Callback from './components/Callback';
import SilentCallback from './components/SilentCallback';
import ProtectedRoute from './components/ProtectedRoute';
import ActionsList from './components/ActionsList';
import ActionExecute from './components/ActionExecute';
import ConfigViewer from './components/ConfigViewer';

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <AuthProvider>
        <Router>
          <Routes>
            <Route path="/login" element={<Login />} />
            <Route path="/callback" element={<Callback />} />
            <Route path="/silent-callback" element={<SilentCallback />} />
            <Route path="/*" element={
              <ProtectedRoute>
                <Box sx={{ minHeight: '100vh', backgroundColor: '#f5f5f5' }}>
                  <Header />
                  <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
                    <Routes>
                      <Route path="/" element={<ActionsList />} />
                      <Route path="/execute/:actionId" element={<ActionExecute />} />
                      <Route path="/configurations" element={<ConfigViewer />} />
                    </Routes>
                  </Container>
                </Box>
              </ProtectedRoute>
            } />
          </Routes>
        </Router>
      </AuthProvider>
    </ThemeProvider>
  );
}

export default App;
