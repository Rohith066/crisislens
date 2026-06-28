"""
CrisisLens — Phase 3, serving API (FastAPI).

Serves the gold hazard feed over HTTP. Reads the Delta `gold/hazards` table with
delta-rs (the `deltalake` package) — a lightweight, JVM-free Delta reader, NOT
Spark. Spark is for batch/stream processing; a request-path API needs a fast,
light reader. The gold table is small, so we read it and filter in-process.

Endpoints:
  GET /health
  GET /hazards?lat=&lon=&radius_km=&limit=   -> hazards near a point, nearest first

Run (from the project root):
  ./.venv/bin/uvicorn serving.api:app --reload
"""

import numpy as np
import pandas as pd
from pathlib import Path

from deltalake import DeltaTable
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse

try:
    from serving.rag import answer as rag_answer
except Exception:   # RAG deps (torch/faiss/sentence-transformers) absent in lean deploys
    rag_answer = None

GOLD_HAZARDS_PATH = "lakehouse/gold/hazards"
EARTH_RADIUS_KM = 6371.0

app = FastAPI(title="CrisisLens API", version="0.1")
STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
def index():
    """Serve the Leaflet map UI."""
    return FileResponse(STATIC_DIR / "index.html")


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance (km) from one point (lat1, lon1) to arrays (lat2, lon2)."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def _clean(v):
    """Make one value JSON-safe: NaN/NaT -> None, Timestamp -> ISO, numpy -> native."""
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(v, np.floating):
        return float(v)
    if isinstance(v, np.integer):
        return int(v)
    return v


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/hazards")
def hazards(
    lat: float = Query(..., ge=-90, le=90, description="latitude of the point of interest"),
    lon: float = Query(..., ge=-180, le=180, description="longitude of the point of interest"),
    radius_km: float = Query(500, gt=0, le=20000, description="search radius in km"),
    limit: int = Query(50, gt=0, le=500, description="max hazards to return"),
):
    """Return hazards within `radius_km` of (lat, lon), nearest first."""
    df = DeltaTable(GOLD_HAZARDS_PATH).to_pandas()

    # vectorized distance from the query point to every hazard
    df["distance_km"] = haversine_km(lat, lon, df["lat"].to_numpy(), df["lon"].to_numpy())
    near = df[df["distance_km"] <= radius_km].sort_values("distance_km").head(limit).copy()
    near["distance_km"] = near["distance_km"].round(1)

    cols = ["event_id", "source", "hazard_type", "severity", "severity_level",
            "lat", "lon", "geo_source", "place", "occurred_at", "title", "url", "distance_km"]
    results = [{k: _clean(v) for k, v in row.items()} for row in near[cols].to_dict("records")]

    return {
        "query": {"lat": lat, "lon": lon, "radius_km": radius_km},
        "count": len(results),
        "hazards": results,
    }


@app.get("/ask")
def ask(
    q: str = Query(..., min_length=3, description="natural-language question"),
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(300, gt=0, le=20000),
):
    """Geo-aware RAG: answer `q` grounded in hazards near (lat, lon), with citations."""
    if rag_answer is None:   # lean/hosted deployment without the local LLM stack
        return {
            "query": {"q": q, "lat": lat, "lon": lon, "radius_km": radius_km},
            "answer": ("The AI assistant runs only in the full local stack (it needs a local LLM). "
                       "This hosted demo serves the live hazard map and the /hazards feed."),
            "citations": [], "used_llm": False,
        }
    result = rag_answer(q, lat, lon, radius_km)
    return {"query": {"q": q, "lat": lat, "lon": lon, "radius_km": radius_km}, **result}

