FROM python:3.13-slim AS builder

# Install uv
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy dependency files and README for build metadata
COPY pyproject.toml uv.lock README.md ./

# Install dependencies (uv build backend needs README.md present)
RUN uv sync --frozen --no-dev

FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Copy installed packages from builder
COPY --from=builder /app/.venv /app/.venv

# Make sure we use venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy application code
COPY . ./

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/')" || exit 1

# Start the application
CMD ["sh", "-c", "uvicorn pipeline.app:app --host 0.0.0.0 --port ${PORT:-8080}"] 