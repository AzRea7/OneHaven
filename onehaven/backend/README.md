# OneHaven

## What this is
A minimal, API-first lead ingestion + scoring engine focused on SE Michigan.

## Run
1) Create `.env` from `.env.example`
2) `uvicorn app.main:app --reload --port 8000`
3) `POST /jobs/refresh?region=se_michigan`
4) `GET /leads/top?zip=48009&strategy=rental&limit=25`

## Notes
- Connectors are intentionally pluggable; replace stubs with real providers over time.
- v0 scoring uses heuristics; swap in your LightGBM models later.
