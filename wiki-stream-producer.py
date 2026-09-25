# Ingestion Layer: Main script streaming Wikimedia changes to Kafka
# Wikimedia SSE -> Kafka

import json
import time
from requests_sse import EventSource
from kafka import KafkaProducer

url = "https://stream.wikimedia.org/v2/stream/recentchange"

headers = {
    "User-Agent": "my-wikipedia-stream-project/1.0"
}

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer = lambda v: json.dumps(v).encode('utf-8'), #raw byte 
    api_version=(3, 4, 0)
)

TOPIC_NAME='wiki_edits'

while True: #infinite loop
    try:
        with EventSource(url, headers=headers) as stream:
            print("Successfully connected! Listening for English Wikipedia changes...\n")
            for event in stream:
                if event.type != "message":
                    continue

                try:
                    event_data = json.loads(event.data)
                except json.JSONDecodeError:
                    continue

                # Ignore Wikimedia's artificial canary events
                if event_data.get("meta", {}).get("domain") == "canary": #synthetic "tests" into the live stream to check system health
                    continue
                if event_data.get('wiki')=='enwiki':
                    payload={
                    "ts" : event_data.get("timestamp"), #.get("") to prevent KeyValue Error 
                    "t" : event_data.get("type"),
                    "server_name" : event_data.get("server_name"),
                    "user" : event_data.get("user"),
                    "title" : event_data.get("title")
                    }

                    # Send the dictionary payload to Kafka    
                    producer.send(topic=TOPIC_NAME, value=payload)
                    print(f"Send to Kafka -> Page: {payload['title']} | User: {payload['user']}")

    except Exception as e:
        print(f"Connection interrupted {e}, retrying in 5 seconds ...")
        time.sleep(5)

# source venv/bin/activate
# docker compose up -d
# python wiki_stream_producer.py

# import json
# import time
# from requests_sse import EventSource

# url = "https://stream.wikimedia.org/v2/stream/recentchange"

# headers = {
#     "User-Agent": "my-wikipedia-stream-project/1.0"
# }
# while True: #infinite loop
#     try:
#         with EventSource(url, headers=headers) as stream:
#             print("Successfully connected! Listening for English Wikipedia changes...\n")
#             for event in stream:
#                 if event.type != "message":
#                     continue

#                 try:
#                     event_data = json.loads(event.data)
#                 except json.JSONDecodeError:
#                     continue

#                 # Ignore Wikimedia's artificial canary events
#                 if event_data.get("meta", {}).get("domain") == "canary": #synthetic "tests" into the live stream to check system health
#                     continue
#                 if event_data.get('wiki')=='enwiki':
#                     ts = event_data.get("timestamp") #.get("") to prevent KeyValue Error 
#                     t = event_data.get("type")
#                     server_name = event_data.get("server_name")
#                     user = event_data.get("user")
#                     title = event_data.get("title")

#                     print(f'Timestamp: {ts} | Server Name: {server_name} | User: {user} | Title: {title} | Type: {t}')

#     except Exception as e:
#         print(f"Connection interrupted {e}, retrying in 5 seconds ...")
#         time.sleep(5)
