# Stream Processing Layer: Consumer reading from Kafka
# Kafka -> Consumer & State Tracking

import json
from kafka import KafkaConsumer
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

TOPIC_NAME = 'wiki_edits'

# Initialize the Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',    # Read from the beginning if no offset exists yet
    enable_auto_commit=True,         # Automatically mark messages as read
    group_id='wiki-consumer-group-v2',  # Consumer group ID for tracking offsets
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Decode bytes back to python dict
)

print(f"Connected to Kafka! Listening for messages on topic '{TOPIC_NAME}'...\n")

# Simple in-memory state tracking to count edits per user
user_edit_counts = {}

try:
    for message in consumer: #infinite loop
        event = message.value
        
        user = event.get('user', 'Unknown')
        title = event.get('title', 'Unknown')
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

        # Update our running state counter
        user_edit_counts[user] = user_edit_counts.get(user, 0) + 1

        # Print the live stream event along with the user's running total
        print(f"[{timestamp}] User: {user} | Page: {title} | Total User Edits: {user_edit_counts[user]}")

except KeyboardInterrupt:
    print("\nStopping consumer gracefully...")
finally:
    consumer.close()
    print("Kafka consumer connection closed successfully.")