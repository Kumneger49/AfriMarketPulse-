#!/bin/bash
# AfriMarket Pulse — daily pipeline
# Runs: ingest → dbt run → dbt test
# Scheduled via cron to run every day at 8am

set -e  # exit immediately if any command fails

PROJECT_DIR="/Users/kumnegermatewos/Desktop/AnalyticsEngineering/AfriMarket_Pulse"

echo "========================================="
echo "AfriMarket Pulse pipeline started: $(date)"
echo "========================================="

# Step 1: ingest fresh data into Snowflake RAW
echo "[1/3] Running ingestion..."
"$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/ingestion/ingest.py"

# Step 2: rebuild dbt models (STAGING views + MARTS tables)
echo "[2/3] Running dbt models..."
"$PROJECT_DIR/venv/bin/dbt" run \
  --project-dir "$PROJECT_DIR/dbt_project" \
  --profiles-dir "$PROJECT_DIR/dbt_project"

# Step 3: run all 19 dbt tests — pipeline fails loudly if data quality breaks
echo "[3/3] Running dbt tests..."
"$PROJECT_DIR/venv/bin/dbt" test \
  --project-dir "$PROJECT_DIR/dbt_project" \
  --profiles-dir "$PROJECT_DIR/dbt_project"

echo "Pipeline complete: $(date)"
