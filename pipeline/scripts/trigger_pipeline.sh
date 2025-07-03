#!/usr/bin/env bash
set -euo pipefail

# Script to remotely trigger the Amazon Keepa pipeline
# Usage: trigger_pipeline.sh [--project PROJECT_ID] [--region REGION] [--service SERVICE_NAME]

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-keepa-api}"

# Parse optional arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT_ID="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --service) SERVICE_NAME="$2"; shift 2;;
    *) echo "Unknown option: $1"; exit 1;;
  esac
done

echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Service: $SERVICE_NAME"

# Get the service URL
SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --format 'value(status.url)')
echo "Service URL: $SERVICE_URL"

# Acquire an identity token
echo "Acquiring identity token..."
TOKEN=$(gcloud auth print-identity-token)

# Trigger the pipeline
echo "Triggering pipeline..."
RESPONSE=$(curl -s -w "\nHTTP_STATUS:%{http_code}" \
  -X POST "${SERVICE_URL}/trigger" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json")
HTTP_STATUS=$(echo "$RESPONSE" | sed -n 's/.*HTTP_STATUS://p')
BODY=$(echo "$RESPONSE" | sed '/HTTP_STATUS:/d')

if [[ "$HTTP_STATUS" -ne 200 ]]; then
  echo "Error triggering pipeline (HTTP $HTTP_STATUS)"
  echo "$BODY"
  exit 1
fi

echo "Pipeline triggered successfully:"
echo "$BODY" 