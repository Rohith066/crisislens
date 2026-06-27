"""
CrisisLens — NWS/NOAA weather-alerts producer (Phase 1, source #2).

Polls the National Weather Service active-alerts API, normalizes each alert to
the CrisisLens canonical event schema, dedups by alert id, and publishes new
alerts to the Kafka topic `weather-alerts` on localhost:9092.

Contrast with the USGS producer (this is the lesson):
  - NWS wants a descriptive User-Agent header (with contact info).
  - severity is CATEGORICAL (Extreme/Severe/Moderate/Minor/Unknown), not numeric.
  - geometry is often null (alerts cover forecast zones, not points); rest are polygons.
  - timestamps are ISO-8601 with offset, not epoch milliseconds.
  - updates arrive as a NEW id (not a bumped 'updated'), so dedup is plain id-based.

Run:  python producers/nws_producer.py
Stop: Ctrl+C
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

# --- Configuration -----------------------------------------------------------
NWS_URL = "https://api.weather.gov/alerts/active"
# NWS asks every client to identify itself; requests' default UA can be rejected.
USER_AGENT = "CrisisLens/0.1 (soumithreddy3003@gmail.com)"
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "weather-alerts"
POLL_INTERVAL_SECONDS = 60
SEEN_PATH = "state/seen_nws.json"   # {alert_id: sent_timestamp}

# Map NWS categorical severity -> a number so `severity` stays the same TYPE as
# the USGS magnitude. (Cross-source comparability is a Phase 2/3 triage concern.)
SEVERITY_SCALE = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1, "Unknown": None}

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "nws-producer",
    "acks": "all",
    "enable.idempotence": True,
}


# --- Dedup state (survives restarts) ----------------------------------------
def load_seen():
    try:
        with open(SEEN_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_seen(seen):
    os.makedirs(os.path.dirname(SEEN_PATH), exist_ok=True)
    tmp = SEEN_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(seen, f)
    os.replace(tmp, SEEN_PATH)  # atomic: never a half-written file


# --- Step 1: fetch -----------------------------------------------------------
def fetch_alerts():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}
    response = requests.get(NWS_URL, headers=headers, timeout=20)
    response.raise_for_status()
    return response.json()["features"]


# --- helpers -----------------------------------------------------------------
def to_utc_iso(ts):
    """ISO-8601 string with offset (e.g. ...-06:00) -> 'YYYY-MM-DDTHH:MM:SSZ' in UTC."""
    if not ts:
        return None
    dt = datetime.fromisoformat(ts)               # parses the timezone offset
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def centroid(geometry):
    """A representative (lat, lon) for any GeoJSON geometry, or (None, None).
    NWS alerts are usually polygons (or null); averaging the vertices is a good-enough
    map pin. Walks the nested coordinate arrays to find every [lon, lat] pair."""
    if not geometry:
        return None, None
    pts = []

    def walk(node):
        if isinstance(node, list):
            if len(node) >= 2 and all(isinstance(v, (int, float)) for v in node[:2]):
                pts.append((node[0], node[1]))    # GeoJSON pair is [lon, lat]
            else:
                for child in node:
                    walk(child)

    walk(geometry["coordinates"])
    if not pts:
        return None, None
    lon = sum(p[0] for p in pts) / len(pts)
    lat = sum(p[1] for p in pts) / len(pts)
    return round(lat, 4), round(lon, 4)


# --- Step 2: normalize -------------------------------------------------------
def normalize(feature):
    props = feature["properties"]
    lat, lon = centroid(feature.get("geometry"))

    # Keep the safety instruction with the text — Phase 3's RAG will want it.
    description = props.get("description") or ""
    if props.get("instruction"):
        description += f"\n\nInstructions: {props['instruction']}"

    return {
        "event_id":    props["id"],                        # CAP urn, unique per message
        "source":      "nws",
        "hazard_type": (props.get("event") or "alert").lower(),
        "title":       props.get("headline") or props.get("event"),
        "severity":    SEVERITY_SCALE.get(props.get("severity")),
        "lat":         lat,
        "lon":         lon,
        "place":       props.get("areaDesc"),
        "zones":       props.get("affectedZones") or [],  # zone URLs -> coords when geometry is null
        "occurred_at": to_utc_iso(props.get("onset") or props.get("effective") or props.get("sent")),
        "updated_at":  to_utc_iso(props.get("sent")),
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "url":         props.get("web") or feature.get("id"),
        "description": description.strip(),
    }


# --- Step 3: delivery callback (errors only — this source is high-volume) -----
def delivery_report(err, msg):
    if err is not None:
        print(f"  ✗ delivery FAILED for {msg.key().decode()}: {err}")


# --- Step 4: main loop -------------------------------------------------------
def main():
    producer = Producer(PRODUCER_CONFIG)
    seen = load_seen()
    print(f"NWS producer started. Loaded {len(seen)} known alerts from {SEEN_PATH}.")
    print(f"Publishing new alerts to '{TOPIC}'. Ctrl+C to stop.\n")

    try:
        while True:
            features = fetch_alerts()
            new_count = 0

            for feature in features:
                event_id = feature["properties"]["id"]
                if event_id in seen:
                    continue                  # NWS issues a NEW id per update -> id dedup is enough

                event = normalize(feature)
                producer.produce(
                    topic=TOPIC,
                    key=event_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                seen[event_id] = feature["properties"].get("sent")
                new_count += 1
                producer.poll(0)

            producer.flush()
            save_seen(seen)
            stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"[{stamp}] polled NWS: {len(features)} active alerts, "
                  f"{new_count} new. Sleeping {POLL_INTERVAL_SECONDS}s...\n")
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping — flushing remaining messages...")
    finally:
        producer.flush()
        save_seen(seen)
        print("Done.")


if __name__ == "__main__":
    main()
