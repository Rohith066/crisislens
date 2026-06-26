"""
CrisisLens — Phase 3, retrieval core for the geo-aware RAG.

"Geo-aware" = spatial filter FIRST, then semantic ranking:
  1. keep only hazards within radius_km of the user (haversine)
  2. rank those survivors by embedding similarity to the question (FAISS)

This ordering is the differentiator: distance narrows the candidates, then meaning
ranks them — so "is there flooding near me?" returns nearby floods, not a semantically
similar flood 2000 km away.

Embeddings: local sentence-transformers (free). Vector search: FAISS. No API keys.
"""

import numpy as np
import faiss
from deltalake import DeltaTable
from sentence_transformers import SentenceTransformer

GOLD_HAZARDS_PATH = "lakehouse/gold/hazards"
EMBED_MODEL = "all-MiniLM-L6-v2"   # 384-dim, small + fast, runs on CPU
EARTH_RADIUS_KM = 6371.0

_model = None
_hazards = None       # gold hazards as a pandas DataFrame (with a 'text' column)
_embeddings = None    # np.float32 [N, 384], L2-normalized so dot product == cosine


def _get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBED_MODEL)
    return _model


def build_index():
    """Load gold hazards and embed each one's text. Call once at startup."""
    global _hazards, _embeddings
    df = DeltaTable(GOLD_HAZARDS_PATH).to_pandas().reset_index(drop=True)
    df["text"] = (df["title"].fillna("") + ". " + df["description"].fillna("")).str.strip()
    _embeddings = _get_model().encode(
        df["text"].tolist(), normalize_embeddings=True, show_progress_bar=False
    ).astype("float32")
    _hazards = df
    return len(df)


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def retrieve(question, lat, lon, radius_km=500, k=5):
    """Return up to k hazards near (lat, lon) most relevant to `question`."""
    if _hazards is None:
        build_index()
    df = _hazards

    # 1) SPATIAL filter — narrow to hazards within the radius.
    dist = haversine_km(lat, lon, df["lat"].to_numpy(), df["lon"].to_numpy())
    cand_idx = np.where(dist <= radius_km)[0]
    if len(cand_idx) == 0:
        return []

    # 2) SEMANTIC rank — FAISS inner-product search over just the survivors.
    cand_emb = _embeddings[cand_idx]
    index = faiss.IndexFlatIP(cand_emb.shape[1])   # vectors are normalized -> IP == cosine
    index.add(cand_emb)
    qvec = _get_model().encode([question], normalize_embeddings=True).astype("float32")
    scores, rows = index.search(qvec, min(k, len(cand_idx)))

    results = []
    for score, row in zip(scores[0], rows[0]):
        h = df.iloc[cand_idx[row]]
        results.append({
            "event_id": h["event_id"],
            "source": h["source"],
            "hazard_type": h["hazard_type"],
            "severity_level": h["severity_level"],
            "place": h["place"],
            "distance_km": round(float(dist[cand_idx[row]]), 1),
            "similarity": round(float(score), 3),
            "title": h["title"],
            "url": h["url"],
            "description": h["description"],
            "lat": float(h["lat"]),
            "lon": float(h["lon"]),
        })
    return results
