#!/usr/bin/env bash
set -euo pipefail

# Script to schedule the keepa-sync Cloud Run job via Cloud Scheduler
# Usage: schedule_keepa_sync.sh [--project PROJECT_ID] [--region REGION] [--job JOB_NAME] [--schedule CRON_SCHEDULE] [--schedule-name SCHED_NAME] [--time-zone TIME_ZONE]

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
JOB_NAME="${JOB_NAME:-keepa-sync}"
SCHED_NAME="${SCHED_NAME:-keepa-sync-schedule}"
CRON_SCHEDULE="${CRON_SCHEDULE:-0 3 * * *}"  # Default: daily at 03:00 UTC
TIME_ZONE="${TIME_ZONE:-UTC}"

# Determine Cloud Scheduler service agent
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
SERVICE_ACCOUNT="service-${PROJECT_NUMBER}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"

echo "Granting Cloud Scheduler service account permission to invoke job '$JOB_NAME'..."
gcloud run jobs add-iam-policy-binding "$JOB_NAME" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/run.invoker"

echo "Creating or updating Cloud Scheduler job '$SCHED_NAME'..."
# Check if job exists
if gcloud scheduler jobs describe "$SCHED_NAME" --project="$PROJECT_ID" --location="$REGION" &>/dev/null; then
  echo "Updating existing scheduler job..."
  gcloud scheduler jobs update http "$SCHED_NAME" \
    --project="$PROJECT_ID" \
    --location="$REGION" \
    --schedule="$CRON_SCHEDULE" \
    --time-zone="$TIME_ZONE" \
    --http-method=POST \
    --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/$JOB_NAME:run" \
    --oidc-service-account-email="$SERVICE_ACCOUNT" \
    --message-body '{}' 
else
  echo "Creating new scheduler job..."
  gcloud scheduler jobs create http "$SCHED_NAME" \
    --project="$PROJECT_ID" \
    --location="$REGION" \
    --schedule="$CRON_SCHEDULE" \
    --time-zone="$TIME_ZONE" \
    --http-method=POST \
    --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/$JOB_NAME:run" \
    --oidc-service-account-email="$SERVICE_ACCOUNT" \
    --message-body '{}' 
fi

echo "Cloud Scheduler job scheduled: $SCHED_NAME runs $JOB_NAME every '$CRON_SCHEDULE' in $TIME_ZONE." 