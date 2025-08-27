#!/bin/bash

echo "🧪 Testing Gundi Integration Web UI..."

# Test 1: Check if containers are running
echo "📋 Checking container status..."
docker compose ps | grep -E "(fastapi|web-ui)" | head -2

# Test 2: Check if FastAPI is responding
echo "🔌 Testing FastAPI endpoint..."
if curl -s http://localhost:8080/v1/actions/ > /dev/null; then
    echo "✅ FastAPI is responding"
    echo "📋 Available actions:"
    curl -s http://localhost:8080/v1/actions/ | jq -r '.[]' | sed 's/^/  - /'
else
    echo "❌ FastAPI is not responding"
fi

# Test 3: Check if web UI is serving
echo "🌐 Testing web UI..."
if curl -s http://localhost:3000 > /dev/null; then
    echo "✅ Web UI is serving"
else
    echo "❌ Web UI is not serving"
fi

# Test 4: Check recent logs
echo "📝 Recent web UI logs:"
docker compose logs web-ui --tail=5

echo ""
echo "🎉 Test complete! Visit http://localhost:3000 to use the web UI"
