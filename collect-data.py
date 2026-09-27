import json
from kafka import KafkaConsumer
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

TOPIC_NAME = "wiki_edits"
OUTPUT_FILE = "historical-data.json"
TARGET_COUNT = 12000  # Collect 12,000 raw records

def collect_data():
    print(f"Connecting to Kafka topic '{TOPIC_NAME}'...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",  ## latest - grab whatever is streaming rn
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    collected_events = []
    print(f"Collecting {TARGET_COUNT} edits...")

    try:
        for message in consumer:
            event = message.value
            collected_events.append(event)

            raw_timestamp = event.get('ts', 'Unknown')
            # Define Pacific Time zone
            pt_zone = ZoneInfo('America/Los_Angeles')
            # Convert epoch timestamp to a clean, readable UTC string
            if raw_timestamp:
                # dt_obj = datetime.fromtimestamp(raw_timestamp, tz=timezone.utc)
                dt_obj = datetime.fromtimestamp(raw_timestamp, tz=pt_zone)
                timestamp = dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC')
            else:
                timestamp = 'Unknown'
            
            print(f"Collected [{len(collected_events)}/{TARGET_COUNT}] - Page: {event.get('title')} | Timestamp: {timestamp}")

            if len(collected_events) >= TARGET_COUNT:
                break
    except KeyboardInterrupt:
        print("\nCollection stopped manually.")

    # Save to JSON file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f: #write mode
        json.dump(collected_events, f, ensure_ascii=False, indent=4)

    print(f"\nSuccess! Saved {len(collected_events)} records to {OUTPUT_FILE}")
    consumer.close()

if __name__ == "__main__":
    collect_data()