#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${0}")" && pwd)"
cd "${SCRIPT_DIR}"/..

# Test the Docker container locally
# Usage: ./test_container.sh

set -e

echo "🔨 Building Docker image..."
docker build -t keepa-pipeline:test .

echo "🧪 Testing container..."
docker run --rm -d \
  --name keepa-test \
  -p 8080:8080 \
  -e PORT=8080 \
  -e GCP_PROJECT_ID=pollen-sandbox-warehouse \
  keepa-pipeline:test

echo "⏳ Waiting for container to start..."
sleep 5

echo "🏥 Testing health check..."
if curl -f http://localhost:8080/; then
  echo "✅ Health check passed!"
else
  echo "❌ Health check failed!"
  docker logs keepa-test
  docker stop keepa-test
  exit 1
fi

echo "📊 Testing status endpoint..."
curl -s http://localhost:8080/status | python3 -m json.tool

echo "🧹 Cleaning up..."
docker stop keepa-test

echo "✅ Container test complete!"
echo "   To run manually: docker run -p 8080:8080 keepa-pipeline:test" 