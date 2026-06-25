"""
CrisisLens — USGS earthquake producer (Phase 1).

Polls the public USGS earthquake feed, normalizes each quake to the CrisisLens
canonical event schema, and publishes NEW or REVISED events to the Kafka topic
`earthquakes` on localhost:9092.

Dedup is based on (event_id, updated): USGS bumps an event's `updated` timestamp
whenever it revises a quake (e.g. a magnitude upgrade), so we republish those but
skip unchanged re-polls. The dedup state is persisted to state/seen.json, so
restarting the producer does NOT republish the whole window.

Run:  python producers/usgs_producer.py
Stop: Ctrl+C
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

# --- Configuration -----------------------------------------------------------
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "earthquakes"
POLL_INTERVAL_SECONDS = 60          # USGS refreshes ~every minute
SEEN_PATH = "state/seen.json"       # persisted dedup state: {event_id: updated_ms}

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "usgs-producer",
    "acks": "all",               # wait for all in-sync replicas to ack the write
    "enable.idempotence": True,  # retries won't create duplicate messages
}


# --- Dedup state (survives restarts) ----------------------------------------
def load_seen():
    """Load {event_id: last-published 'updated' ms} from disk; {} if no file yet."""
    try:
        with open(SEEN_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_seen(seen):
    """Write the seen-map atomically: temp file + rename, so a crash mid-write
    can never leave a half-written (corrupt) JSON file behind."""
    os.makedirs(os.path.dirname(SEEN_PATH), exist_ok=True)
    tmp = SEEN_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(seen, f)
    os.replace(tmp, SEEN_PATH)  # atomic on POSIX: readers see old OR new, never partial


# --- Step 1: fetch -----------------------------------------------------------
def fetch_quakes():
    """GET the USGS feed and return the list of GeoJSON 'features' (one per quake)."""
    response = requests.get(USGS_URL, timeout=15)
    response.raise_for_status()          # turn HTTP 4xx/5xx into an exception
    return response.json()["features"]


# --- Step 2: normalize -------------------------------------------------------
def normalize(feature):
    """Map one USGS GeoJSON feature -> the CrisisLens canonical event schema."""
    props = feature["properties"]
    # GeoJSON coordinates are [longitude, latitude, depth] — note the order!
    lon, lat, depth_km = feature["geometry"]["coordinates"]

    # USGS 'time'/'updated' are epoch milliseconds (UTC). Convert to ISO-8601.
    occurred_at = datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated_at = datetime.fromtimestamp(props["updated"] / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "event_id":    feature["id"],
        "source":      "usgs",
        # USGS publishes non-quakes too (explosion, quarry blast, sonic boom).
        # Use the source's own type so we label each event honestly (Option A).
        "hazard_type": (props.get("type") or "earthquake").lower(),
        "title":       props.get("title"),
        "severity":    props.get("mag"),          # earthquake magnitude
        "lat":         lat,
        "lon":         lon,
        "place":       props.get("place"),
        "occurred_at": occurred_at,
        "updated_at":  updated_at,                 # changes when USGS revises the event
        "ingested_at": ingested_at,
        "url":         props.get("url"),
        "description": f"{props.get('title')}. Depth {depth_km} km.",
    }


# --- Step 3: delivery callback ----------------------------------------------
def delivery_report(err, msg):
    """Called once per message by Kafka — confirms the write landed or failed."""
    if err is not None:
        print(f"  ✗ delivery FAILED for {msg.key().decode()}: {err}")
    else:
        print(
            f"  ✓ delivered {msg.key().decode()} "
            f"-> {msg.topic()}[partition {msg.partition()}] @ offset {msg.offset()}"
        )


# --- Step 4: main loop -------------------------------------------------------
def main():
    producer = Producer(PRODUCER_CONFIG)
    seen = load_seen()  # {event_id: 'updated' ms we last published}
    print(f"USGS producer started. Loaded {len(seen)} known events from {SEEN_PATH}.")
    print(f"Publishing new/revised quakes to '{TOPIC}'. Ctrl+C to stop.\n")

    try:
        while True:
            features = fetch_quakes()
            new_count = revised_count = 0

            for feature in features:
                event_id = feature["id"]
                updated = feature["properties"]["updated"]  # epoch ms; bumps on revision

                if event_id in seen:
                    if seen[event_id] >= updated:
                        continue            # same version we already sent -> skip
                    revised_count += 1      # USGS revised this quake -> republish
                else:
                    new_count += 1          # brand-new quake

                event = normalize(feature)
                producer.produce(
                    topic=TOPIC,
                    key=event_id.encode("utf-8"),          # same quake -> same partition
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                seen[event_id] = updated     # remember the version we just published
                producer.poll(0)             # serve delivery callbacks without blocking

            producer.flush()                 # block until this batch is fully acknowledged
            save_seen(seen)                  # persist dedup state AFTER Kafka confirms
            unchanged = len(features) - new_count - revised_count
            stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"[{stamp}] polled USGS: {len(features)} quakes — "
                  f"{new_count} new, {revised_count} revised, {unchanged} unchanged. "
                  f"Sleeping {POLL_INTERVAL_SECONDS}s...\n")
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping — flushing remaining messages...")
    finally:
        producer.flush()   # never lose buffered messages on exit
        save_seen(seen)    # persist whatever we published before exiting
        print("Done.")


if __name__ == "__main__":
    main()
