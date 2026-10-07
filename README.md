# Real-Time Wikipedia Edit Anomaly Detection Pipeline
A production-grade, low-latency streaming pipeline that ingests live English Wikipedia edits, tracks rolling user velocity using Redis, and flags vandalism or spam in real time using a machine learning Isolation Forest model.

 ### Architecture & Tech Stack
- Ingestion: Apache Kafka (streaming live edit events from the Wikimedia event stream)
- State Management: Redis (in-memory sliding window tracker using Sorted Sets / ZSET)
- Containerization & Orchestration: Docker & Docker Compose (managing Kafka and Redis infrastructure)
- Inference Engine: Scikit-Learn IsolationForest (serialized via joblib)
- Language: Python

### How It Works
- Event Stream Ingestion: The Kafka consumer reads live Wikipedia edit packets, extracting structural features such as text length info, comment length, topic, bot flags etc.

- Stateful Velocity Tracking (Redis):
   - Maintains a rolling 60-second edit history for each unique user using Redis Sorted Sets `(user_velocity:{username})`.
   - Automatically purges expired timestamps using `ZREMRANGEBYSCORE` and applies pipeline batching `r.pipeline()` for sub-millisecond throughput.

- Machine Learning Inference: Passes the combined feature vector into an Isolation Forest model to score the edit. \
  1: Normal human or standard bot activity \
  -1: Anomalous or suspicious activity flagged for review

### Getting Started
Prerequisites:
- Python 3.9+
- Docker & Docker Compose installed and running

1. Clone the Repository \
`git clone https://github.com/urvimidha/wiki-stream-pipeline.git` \
`cd wiki-stream-pipeline`

2. Create Virtual Environment \
`python -m venv venv`

3. Activate the Environment and Install Python Dependencies \
`source venv/bin/activate` \
`pip install -r requirements.txt`

4. Start Infrastructure (Kafka & Redis) \
`docker compose up -d`
`docker ps #check status`

5. Run the Pipeline \
Start the Kafka producer to stream Wikipedia edits: \
`python producer.py`

   In a separate terminal, start the consumer to process events and track anomalies: \
`python stream_consumer.py`
