import json
import pandas as pd
import numpy as np
from collections import defaultdict, deque
from sklearn.ensemble import IsolationForest
import joblib

DATA_FILE = "historical-data.json"
MODEL_OUTPUT = "anomaly-model.pkl"

def train_model():
    print(f"Loading historical data from {DATA_FILE}...")
    with open(DATA_FILE, "r", encoding="utf-8") as f: #read mode
        data = json.load(f) #data - list

    # Sort historical data chronologically by timestamp so rolling windows make sense
    data = sorted(data, key=lambda x: x.get("ts", 0))

    print(f"Loaded {len(data)} records. Calculating features and historical velocities...")

    # Dictionary to track recent edit timestamps per user: user_name -> deque of timestamps
    user_edit_history = defaultdict(deque)
    WINDOW_SECONDS = 60

    features = []
    for event in data:
        timestamp = event.get("ts", 0)
        user = event.get("user", "Unknown")

        # --- 1. Calculate Historical User Velocity ---
        history = user_edit_history[user]
        # Remove timestamps older than 60 seconds from the current event
        while history and (timestamp - history[0]) > WINDOW_SECONDS:
            history.popleft()
        
        # Current velocity is how many edits this user made in the prior 60 seconds
        user_velocity = len(history)
        
        # Record this current edit's timestamp into their history
        history.append(timestamp)

        # --- 2. Extract Structural Features ---
        length_info = event.get("length", {"old": 0, "new": 0})
        length_delta = length_info.get("new", 0) - length_info.get("old", 0)
        
        comment = event.get("comment", "")
        comment_length = len(comment) if comment else 0

        is_bot = 1 if event.get("bot", False) else 0
        is_minor = 1 if event.get("minor", False) else 0

        # Append complete 5-feature vector
        features.append([
            length_delta,
            comment_length,
            is_bot,
            is_minor,
            user_velocity
        ])

    X = np.array(features)

    print("Training Isolation Forest model on 5 features...")
    model = IsolationForest(contamination=0.01, random_state=42, n_estimators=100)
    model.fit(X)

    joblib.dump(model, MODEL_OUTPUT)
    print(f"Success! Model trained with velocity and saved to {MODEL_OUTPUT}")

if __name__ == "__main__":
    train_model()