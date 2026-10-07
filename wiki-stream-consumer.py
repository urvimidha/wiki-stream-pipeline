# Stream Processing Layer: Consumer reading from Kafka
# Kafka -> Consumer & State Tracking

import json
import time
import redis
import joblib
import numpy as np
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from kafka import KafkaConsumer

# Configuration Constants
TOPIC_NAME = "wiki_edits"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
MODEL_PATH = "anomaly-model.pkl"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
WINDOW_SECONDS = 60

def run_consumer():
    print("Loading Isolation Forest model...")
    model = joblib.load(MODEL_PATH)
    print("Model loaded successfully")

    print("Connecting to Redis...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping() # Verify connection
    print("Connected to Redis")

    print(f"Connecting to Kafka topic '{TOPIC_NAME}'...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest", # Start reading live incoming messages
        # auto_offset_reset='earliest', # Read from the beginning if no offset exists yet
        group_id='wiki-consumer-group-v1',  # Consumer group ID for tracking offsets
        enable_auto_commit=True # Automatically mark messages as read
    )
    
    print(f"Connected to Kafka! Listening for messages on topic '{TOPIC_NAME}'...\n")

    for message in consumer:
        event = message.value
        raw_timestamp = event.get("ts", time.time())
        user = event.get("user", "Unknown")

        # --- 1. Real-Time Velocity Tracking via Redis (Sorted Set) ---
        user_key = f"user_velocity:{user}"
    
        pipe = r.pipeline() # Redis pipeline (temp container) - cuts network back and forth chatter
        pipe.zremrangebyscore(user_key, 0, raw_timestamp - WINDOW_SECONDS) # removes timestamps older than 60 seconds
        pipe.zadd(user_key, {str(raw_timestamp): raw_timestamp}) # add the current edit timestamp
        pipe.zcard(user_key) # Zet CARDinality - how many items are currently inside this set (60 sec)
        pipe.expire(user_key, WINDOW_SECONDS * 2) # Set a TTL (Time-To-Live) so inactive users clean themselves up automatically
        
        _, _, user_velocity, _ = pipe.execute()

        # --- 2. Extract Structural Features ---
        length_info = event.get("length", {"old": 0, "new": 0})
        length_delta = length_info.get("new", 0) - length_info.get("old", 0)
        
        comment = event.get("comment", "")
        comment_length = len(comment) if comment else 0

        is_bot = 1 if event.get("bot", False) else 0
        is_minor = 1 if event.get("minor", False) else 0

        # Construct feature vector matching training structure
        features = np.array([[
            length_delta,
            comment_length,
            is_bot,
            is_minor,
            user_velocity
        ]])

        # --- 3. Score and Predict via Isolation Forest ---
        prediction = model.predict(features)[0]  # Returns 1 (normal) or -1 (anomaly)
        anomaly_score = model.decision_function(features)[0]  # Continuous score

        # --- 4. Alerting Logic ---
        page_title = event.get("title", "Unknown Page")
        if prediction == -1:
            print('\n')
            print('[ANOMALY DETECTED]')
            print(f"User: {user} | Page: {page_title} | Velocity: {user_velocity} edits/min | Is Bot: {is_bot} | Is Bot: {is_bot} | Length Delta: {length_delta} | Comment Length: {comment_length} | Score: {anomaly_score:.4f}")
            print(f"Comment: {comment}")
            print(f"Length Info: {length_info}")
            print('\n')
        else:
            # Optional: print regular edits
            print(f"User: {user} | Page: {page_title} | Velocity: {user_velocity} edits/min | Is Bot: {is_bot} | Length Delta: {length_delta} | Comment Length: {comment_length} | Score: {anomaly_score:.4f}")
            # pass


if __name__ == "__main__":
    run_consumer()
















# import json
# from kafka import KafkaConsumer
# from datetime import datetime, timezone
# from zoneinfo import ZoneInfo

# TOPIC_NAME = 'wiki_edits'

# # Initialize the Kafka Consumer
# consumer = KafkaConsumer(
#     TOPIC_NAME,
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',    # Read from the beginning if no offset exists yet
#     enable_auto_commit=True,         # Automatically mark messages as read
#     group_id='wiki-consumer-group-v1',  # Consumer group ID for tracking offsets
#     value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Decode bytes back to python dict
# )

# print(f"Connected to Kafka! Listening for messages on topic '{TOPIC_NAME}'...\n")

# # Simple in-memory state tracking to count edits per user
# user_edit_counts = {}

# try:
#     for message in consumer: #infinite loop
#         event = message.value
        
#         user = event.get('user', 'Unknown')
#         title = event.get('title', 'Unknown')
#         raw_timestamp = event.get('ts', 'Unknown')

#         # Define Pacific Time zone
#         pt_zone = ZoneInfo('America/Los_Angeles')

#         # Convert epoch timestamp to a clean, readable UTC string
#         if raw_timestamp:
#             # dt_obj = datetime.fromtimestamp(raw_timestamp, tz=timezone.utc)
#             dt_obj = datetime.fromtimestamp(raw_timestamp, tz=pt_zone)
#             timestamp = dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC')
#         else:
#             timestamp = 'Unknown'

#         # Update our running state counter
#         user_edit_counts[user] = user_edit_counts.get(user, 0) + 1

#         # Print the live stream event along with the user's running total
#         print(f"[{timestamp}] User: {user} | Page: {title} | Total User Edits: {user_edit_counts[user]}")
        
# except KeyboardInterrupt:
#     print("\nStopping consumer gracefully...")
# finally:
#     consumer.close()
#     print("Kafka consumer connection closed successfully.")