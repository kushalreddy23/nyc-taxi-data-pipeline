import os
import requests
from tqdm import tqdm

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

FILES = [
    "yellow_tripdata_2024-01.parquet",
    "yellow_tripdata_2024-02.parquet",
    "yellow_tripdata_2024-03.parquet"
]

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
    "raw")
CHUNK_SIZE = 1024 * 1024  # 1 MB


def download_file(filename):
    url = f"{BASE_URL}/{filename}"
    filepath = os.path.join(DATA_DIR, filename)

    if os.path.exists(filepath):
        print(f"{filename} already exists, skipping.")
        return

    print(f"Downloading {filename}...")

    try:
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()  # Raises error for bad responses

            total_size = int(response.headers.get("content-length", 0))

            with open(filepath, "wb") as f, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=filename
            ) as progress_bar:

                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        progress_bar.update(len(chunk))

        print(f"Saved to {filepath}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {filename}: {e}")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    for file in FILES:
        download_file(file)


if __name__ == "__main__":
    main()