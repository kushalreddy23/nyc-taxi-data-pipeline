import os

from checkpoint_manager import read_checkpoint

RAW_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
    "raw"
)


def get_all_files():
    files = [
        f for f in os.listdir(RAW_DATA_DIR)
        if f.endswith(".parquet")
    ]

    files.sort()

    return files


def get_new_files():
    last_processed = read_checkpoint()

    all_files = get_all_files()

    if last_processed is None:
        return all_files

    new_files = [
        f for f in all_files
        if f > last_processed
    ]

    return new_files


if __name__ == "__main__":
    new_files = get_new_files()

    print("New files to process:")

    for file in new_files:
        print(file)