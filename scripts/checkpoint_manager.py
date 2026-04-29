import json
import os

CHECKPOINT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "checkpoint.json"
)


def read_checkpoint():
    if not os.path.exists(CHECKPOINT_PATH):
        return None

    with open(CHECKPOINT_PATH, "r") as f:
        data = json.load(f)

    return data.get("last_processed_file")


def update_checkpoint(filename):
    data = {
        "last_processed_file": filename
    }

    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Checkpoint updated: {filename}")


def reset_checkpoint():
    data = {
        "last_processed_file": None
    }

    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(data, f, indent=4)

    print("Checkpoint reset")