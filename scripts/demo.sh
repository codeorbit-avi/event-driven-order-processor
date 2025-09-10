#!/usr/bin/env bash
set -euo pipefail

API=${1:-http://127.0.0.1:8000}

echo "Health:"
curl -s $API/ | jq .

echo "Triggering report..."
RID=$(curl -s -X POST $API/trigger_report | jq -r .report_id)
echo "report_id: $RID"

echo "Polling..."
for i in $(seq 1 30); do
  STATUS=$(curl -s "$API/get_report?report_id=$RID" | jq -r .status || true)
  if [ "$STATUS" = "Running" ] || [ -z "$STATUS" ]; then
    echo "Attempt $i: Running..."
    sleep 1
  else
    break
  fi
done

# Download when complete
echo "Downloading CSV..."
curl -L "$API/get_report?report_id=$RID" -o sample_output/report_example.csv
echo "Saved to sample_output/report_example.csv"
