# 🌊 Amazon Keepa Streaming Pipeline

Self-contained Cloud Run deployment for Amazon Keepa price monitoring and ingestion pipeline.

## 🚀 Quick Deploy

```bash
cd pipeline
# Build, push, and deploy both the FastAPI service and the Cloud Run job
gcloud builds submit . --config cloudbuild.yaml
```

## 📋 Prerequisites

- A **Google Cloud Project** with billing enabled
- **gcloud CLI** installed and authenticated (`gcloud auth login`)
- **Keepa API Key** stored in Secret Manager as `keepa-api-key`
- A **service account** with the following roles:
  - `roles/bigquery.dataEditor`
  - `roles/storage.objectAdmin`
  - `roles/secretmanager.secretAccessor`
  - `roles/run.admin`
  - `roles/cloudscheduler.admin`

## 🏗️ Architecture

This pipeline uses a **streaming architecture** with:
- **Memory-safe**: O(1) memory usage (max 25MB)
- **Fault-tolerant**: Checkpoint-based resume from any interruption
- **Cost-optimized**: 99% reduction in API calls
- **HTTP-triggered**: Manual or scheduled execution

## 📁 Directory Structure

```
pipeline/
├── scripts/                  # Helper scripts for manual trigger & scheduling
│   ├── trigger_pipeline.sh   # Invoke the /trigger endpoint via gcloud + curl
│   └── schedule_keepa_sync.sh# Set up Cloud Scheduler to run keepa-sync daily
├── pipeline/                 # Core Python package
│   ├── __init__.py
│   ├── app.py                # FastAPI service endpoints
│   ├── streaming_daily_pipeline.py  # run_pipeline() logic
│   └── config.py             # Env var & Secret Manager config
├── tests/                    # pytest unit & integration tests
├── Dockerfile                # Multi-stage container build
├── cloudbuild.yaml           # CI/CD: build, push, deploy service & job
├── .dockerignore             # Files to exclude from Docker context
├── .gcloudignore             # Files to exclude from gcloud context
├── pyproject.toml            # Python dependency management
├── uv.lock                   # Locked dependency versions
└── README.md                 # This file
```

## 📦 CI/CD Pipeline (Cloud Build)

Cloud Build (via `cloudbuild.yaml`) performs:

1. **Build** Docker image: `gcr.io/$PROJECT_ID/keepa:latest`
2. **Push** to Container Registry
3. **Deploy** FastAPI **service** `keepa-api` (HTTP-triggered)
4. **Deploy** Cloud Run **job** `keepa-sync` (headless batch)

Build artifacts:

```yaml
images:
  - 'gcr.io/$PROJECT_ID/keepa:latest'
```

## 🌐 API Endpoints

| Path       | Method | Description                    |
|------------|--------|--------------------------------|
| `/`        | GET    | Health check                   |
| `/status`  | GET    | Current pipeline status        |
| `/trigger` | POST   | Start pipeline asynchronously  |

## 🔧 Manual Trigger

Use the helper script or raw gcloud + curl:

```bash
# Make script executable
chmod +x scripts/trigger_pipeline.sh

# Run it (adjust project/region/service if needed)
./scripts/trigger_pipeline.sh \
  --project YOUR_PROJECT_ID \
  --region us-central1 \
  --service keepa-api
```

Or one-off without script:

```bash
URL=$(gcloud run services describe keepa-api \
       --project YOUR_PROJECT_ID \
       --region us-central1 \
       --format='value(status.url)')
TOKEN=$(gcloud auth print-identity-token)

curl -X POST "$URL/trigger" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json"
```

## ⏰ Scheduling (Daily Cron)

When ready to automate, set up Cloud Scheduler to fire the batch job:

```bash
chmod +x scripts/schedule_keepa_sync.sh
./scripts/schedule_keepa_sync.sh \
  --project YOUR_PROJECT_ID \
  --region us-central1 \
  --job keepa-sync \
  --schedule "0 3 * * *" \
  --time-zone "UTC"
```

This script will:

- Grant the Cloud Scheduler agent `roles/run.invoker` on the `keepa-sync` job
- Create (or update) a Scheduler job named `keepa-sync-schedule`
- POST to the Cloud Run Jobs API daily at the given cron

## ⚙️ Configuration & Secrets

- **Secret Name**: `keepa-api-key` in Secret Manager
- **Env Vars** (set via Cloud Run and Jobs):
  - `GCP_PROJECT_ID`  
  - any other pipeline-specific variables

## 🧪 Testing

Run unit and integration tests locally:

```bash
cd pipeline
pm  # if using uv plugin to install deps
pytest --maxfail=1 --disable-warnings -q
pytest --run-integration --run-keepa
```

## 🛠️ Local Development

Install dependencies and run the FastAPI app:

```bash
cd pipeline
pip install uv
uv sync --no-dev           # synchronize virtual env
uvicorn pipeline.app:app --reload --host 127.0.0.1 --port 8000
```

## 🔍 Troubleshooting

- **Container fails to start**: ensure `CMD` in Dockerfile points to `pipeline.app:app`
- **Permission errors**: verify IAM roles for service account and scheduler agent
- **Pipeline errors**: check Cloud Run logs:
  ```bash
gcloud run services logs read keepa-api --region us-central1 --limit 50
```

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