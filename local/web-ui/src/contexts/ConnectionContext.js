import React, { createContext, useContext, useState } from 'react';

const ConnectionContext = createContext();

export const useConnection = () => {
  const context = useContext(ConnectionContext);
  if (!context) {
    throw new Error('useConnection must be used within a ConnectionProvider');
  }
  return context;
};

export const ConnectionProvider = ({ children }) => {
  const [selectedConnection, setSelectedConnection] = useState(null);

  const selectConnection = (connection) => {
    setSelectedConnection(connection);
  };

  const clearSelection = () => {
    setSelectedConnection(null);
  };

  const value = {
    selectedConnection,
    selectConnection,
    clearSelection
  };

  return (
    <ConnectionContext.Provider value={value}>
      {children}
    </ConnectionContext.Provider>
  );
};
