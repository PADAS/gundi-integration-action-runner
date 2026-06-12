#!/bin/bash

# Development script for the Gundi Integration Web UI

echo "🚀 Starting Gundi Integration Web UI in development mode..."

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# Start the development server
echo "🌐 Starting development server..."
echo "The web UI will be available at: http://localhost:3000"
echo "Make sure the FastAPI service is running on port 8080"
echo ""
echo "Press Ctrl+C to stop the development server"
echo ""

npm start
