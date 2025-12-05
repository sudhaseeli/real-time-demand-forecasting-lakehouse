import os
import json
import random
import time
from datetime import datetime, timedelta
import pathlib
import yaml

CONFIG_PATH = pathlib.Path(__file__).resolve().parents[2] / "config" / "streaming_config.yml"

PRODUCT_IDS = [f"P{i:03d}" for i in range(1, 21)]
STORE_IDS = [f"S{i:02d}" for i in range(1, 6)]

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def generate_order_event(order_id: int):
    now = datetime.utcnow()
    event = {
        "order_id": f"O{order_id:06d}",
        "product_id": random.choice(PRODUCT_IDS),
        "store_id": random.choice(STORE_IDS),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 200), 2),
        "order_timestamp": now.isoformat() + "Z"
    }
    return event

def write_local_events(config, num_events: int = 100):
    events_dir = pathlib.Path(config["streaming"]["events_dir"])
    events_dir.mkdir(parents=True, exist_ok=True)
    out_file = events_dir / f"orders_{int(time.time())}.jsonl"
    with open(out_file, "w") as f:
        for i in range(1, num_events + 1):
            evt = generate_order_event(i)
            f.write(json.dumps(evt) + "\n")
    print(f"Wrote {num_events} events to {out_file}")

def main():
    config = load_config()
    mode = config["streaming"]["mode"]
    if mode == "local_files":
        write_local_events(config, num_events=200)
    else:
        # Placeholder for real Kafka producer
        print("Kafka mode selected - implement Kafka producer here.")

if __name__ == "__main__":
    main()
