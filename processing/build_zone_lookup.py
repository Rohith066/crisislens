"""
CrisisLens — Phase 3 geo-enrichment, Step 2: build a zone -> centroid lookup.

Reads silver, collects the unique NWS zone URLs referenced by alerts that have no
coordinates, fetches each zone's polygon from the NWS zones API, computes a
centroid, and caches it to reference/zone_centroids.json.

Re-runnable: zones already in the cache are skipped, and progress is saved every
100 fetches, so an interrupted run resumes cleanly.

Run:  python processing/build_zone_lookup.py
"""

import json
import os
from itertools import chain

import requests
from deltalake import DeltaTable

SILVER_PATH = "lakehouse/silver"
OUT_PATH = "reference/zone_centroids.json"
USER_AGENT = "CrisisLens/0.1 (https://github.com/Rohith066/crisislens)"


def centroid(geometry):
    """Average all vertices of a GeoJSON geometry -> [lat, lon], or None."""
    if not geometry:
        return None
    pts = []

    def walk(node):
        if isinstance(node, list):
            if len(node) >= 2 and all(isinstance(v, (int, float)) for v in node[:2]):
                pts.append((node[0], node[1]))   # [lon, lat]
            else:
                for child in node:
                    walk(child)

    walk(geometry.get("coordinates"))
    if not pts:
        return None
    lat = round(sum(p[1] for p in pts) / len(pts), 4)
    lon = round(sum(p[0] for p in pts) / len(pts), 4)
    return [lat, lon]


def main():
    df = DeltaTable(SILVER_PATH).to_pandas()
    null_coord = df[df["lat"].isna()]
    zone_urls = sorted(set(chain.from_iterable(
        z for z in null_coord["zones"] if z is not None and len(z) > 0)))

    os.makedirs("reference", exist_ok=True)
    cache = json.load(open(OUT_PATH)) if os.path.exists(OUT_PATH) else {}
    todo = [u for u in zone_urls if u not in cache]
    print(f"{len(zone_urls)} unique zones | {len(cache)} cached | {len(todo)} to fetch")

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    ok = fail = 0
    for i, url in enumerate(todo, 1):
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            c = centroid(resp.json().get("geometry"))
            if c:
                cache[url] = c
                ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1
        if i % 100 == 0:
            json.dump(cache, open(OUT_PATH, "w"))
            print(f"  {i}/{len(todo)} (ok={ok} fail={fail})")

    json.dump(cache, open(OUT_PATH, "w"))
    print(f"done: {len(cache)} zones cached -> {OUT_PATH} (this run: ok={ok} fail={fail})")


if __name__ == "__main__":
    main()
