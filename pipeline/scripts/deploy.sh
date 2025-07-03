#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${0}")" && pwd)"
cd "${SCRIPT_DIR}"/..

# Deploy Amazon Keepa Pipeline to Cloud Run
# Usage: ./deploy.sh [SERVICE_NAME] [REGION]

set -e

SERVICE_NAME=${1:-amazon-keepa-pipeline}
REGION=${2:-us-central1}
PROJECT_ID=${GCP_PROJECT_ID:-pollen-sandbox-warehouse}

echo "🚀 Deploying $SERVICE_NAME to Cloud Run..."
echo "   Project: $PROJECT_ID"
echo "   Region: $REGION"
echo "   Source: $(pwd)"

# Build and deploy using gcloud
gcloud run deploy $SERVICE_NAME \
  --source . \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --allow-unauthenticated \
  --memory 512Mi \
  --cpu 1 \
  --timeout 3600 \
  --concurrency 1 \
  --min-instances 0 \
  --max-instances 1 \
  --set-env-vars GCP_PROJECT_ID=$PROJECT_ID

echo "✅ Deployment complete!"
echo "   Service URL: https://$SERVICE_NAME-$REGION.run.app"
echo "   Trigger endpoint: https://$SERVICE_NAME-$REGION.run.app/trigger" 