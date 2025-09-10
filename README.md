# {firstname}_{YYYY-MM-DD}

Backend APIs for store uptime/downtime monitoring (FastAPI + SQLite).  
Generates a CSV report of uptime/downtime for last hour/day/week, strictly within business hours, with timezone-aware interpolation.

## Why this approach?

- **Fresh data**: Ingests CSVs on demand at report time; no stale precomputation.
- **Business hours only**: Converts local business windows to UTC, intersects with analysis window.
- **Interpolation**: Midpoint/Voronoi logic between irregular polls (piecewise-constant).
- **Defaults**: Missing business hours => 24×7; missing timezone => America/Chicago.
- **“Now”**: Fixed to the max `timestamp_utc` in the status CSV (per prompt).

## Run locally

> Debian/Ubuntu note: use a virtualenv (PEP 668).

```bash
./scripts/run_local.sh
