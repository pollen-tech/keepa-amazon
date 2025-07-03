# 🌊 Amazon Keepa Streaming Pipeline

Self-contained Cloud Run deployment for the Amazon Keepa price monitoring pipeline.

## 🚀 Quick Deploy

```bash
cd pipeline/
./deploy.sh
```

## 📋 Prerequisites

1. **Google Cloud Project** with billing enabled
2. **Keepa API Key** stored in Google Secret Manager
3. **gcloud CLI** authenticated with appropriate permissions

## 🏗️ Architecture

This pipeline uses a **streaming architecture** with:
- **Memory-safe**: O(1) memory usage (max 25MB)
- **Fault-tolerant**: Checkpoint-based resume from any interruption
- **Cost-optimized**: 99% reduction in API calls
- **HTTP-triggered**: Manual or scheduled execution

## 📁 Directory Structure

```
pipeline/
├── daily/
│   ├── Dockerfile          # Container configuration
│   └── web_service.py      # FastAPI HTTP service
├── deploy/
│   ├── cloudbuild.yaml     # Cloud Build configuration
│   └── cloud_run_service.yaml  # Cloud Run service spec
├── config/
│   ├── pipeline_config.py  # Pipeline settings
│   └── bigquery_schema.json # BigQuery table schema
├── pyproject.toml          # Python dependencies
├── uv.lock                 # Locked dependencies
├── deploy.sh              # Deployment script
└── README.md              # This file
```

## 🔧 Setup

### 1. Store Keepa API Key

```bash
echo -n "YOUR_KEEPA_API_KEY" | gcloud secrets create keepa-api-key \
    --data-file=- \
    --project=YOUR_PROJECT_ID
```

### 2. Grant Service Account Permissions

```bash
# Create service account if needed
gcloud iam service-accounts create amazon-keepa-pipeline \
    --display-name="Amazon Keepa Pipeline"

# Grant necessary permissions
PROJECT_ID=YOUR_PROJECT_ID
SERVICE_ACCOUNT=amazon-keepa-pipeline@${PROJECT_ID}.iam.gserviceaccount.com

# BigQuery permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.dataEditor"

# Storage permissions (for Parquet staging)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/storage.objectAdmin"

# Secret Manager access
gcloud secrets add-iam-policy-binding keepa-api-key \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor"
```

## 🚀 Deployment

### Deploy to Cloud Run

```bash
cd pipeline/
./deploy.sh
```

The script will:
1. Create GCS staging bucket (if needed)
2. Verify Keepa API key secret exists
3. Build and push container image
4. Deploy to Cloud Run
5. Display service URL and endpoints

### Manual Deployment (Alternative)

```bash
# From pipeline directory
gcloud builds submit . \
    --config=deploy/cloudbuild.yaml \
    --project=YOUR_PROJECT_ID
```

## 🌐 API Endpoints

Once deployed, the service exposes:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check & status |
| `/status` | GET | Pipeline execution status |
| `/trigger` | POST | Start pipeline (async) |
| `/trigger-sync` | POST | Start pipeline (wait for completion) |
| `/logs` | GET | View recent execution logs |

## 🧪 Testing

### Health Check
```bash
curl https://YOUR-SERVICE-URL.run.app/
```

### Trigger Pipeline Manually
```bash
# Async (returns immediately)
curl -X POST https://YOUR-SERVICE-URL.run.app/trigger

# Sync (waits for completion)
curl -X POST https://YOUR-SERVICE-URL.run.app/trigger-sync
```

### Check Status
```bash
curl https://YOUR-SERVICE-URL.run.app/status
```

## ⏰ Scheduling

### Option 1: Cloud Scheduler (Recommended)

```bash
gcloud scheduler jobs create http amazon-keepa-daily \
    --location=us-central1 \
    --schedule="0 2 * * *" \
    --uri="https://YOUR-SERVICE-URL.run.app/trigger" \
    --http-method=POST \
    --oidc-service-account-email=YOUR-SERVICE-ACCOUNT@PROJECT.iam.gserviceaccount.com
```

### Option 2: Manual Triggering

For the first few days, manually trigger via:
- Cloud Console UI
- `curl` commands
- Custom dashboard

## 📊 Monitoring

### BigQuery Progress
```sql
-- Check today's data
SELECT marketplace, category, COUNT(*) as records
FROM `YOUR_PROJECT.amazon_keepa_products.price_history`
WHERE ingestion_date = CURRENT_DATE()
GROUP BY marketplace, category;
```

### Cloud Run Logs
```bash
gcloud run services logs read amazon-keepa-streaming-pipeline \
    --project=YOUR_PROJECT_ID \
    --region=us-central1 \
    --limit=50
```

### GCS Staging Files
```bash
gsutil ls -l gs://YOUR_PROJECT-keepa-staging/price_data/
```

## 🔍 Troubleshooting

### Pipeline Not Starting
- Check Keepa API key secret exists
- Verify service account permissions
- Check Cloud Run logs for errors

### Memory Issues
- Pipeline uses streaming (max 25MB RAM)
- If OOM, check for code modifications

### Resume After Failure
- Pipeline automatically resumes from checkpoint
- Check GCS for `daily_pipeline/state.json`

## 🛡️ Security

- Service runs with minimal permissions
- API key stored in Secret Manager
- Non-root container user
- No public data exposure

## 💰 Cost Optimization

- **API Calls**: ~$2.43/day (99% reduction)
- **Cloud Run**: ~$0.10/day (512MB, 1 CPU)
- **Storage**: ~$0.02/day (Parquet staging)
- **Total**: ~$2.55/day ($930/year)

## 📈 Performance

- **Processing Time**: ~30 minutes for 24,289 ASINs
- **Memory Usage**: Constant 25MB (streaming)
- **API Efficiency**: 243 bulk calls vs 24,289 individual
- **Fault Recovery**: Resume from exact interruption point 