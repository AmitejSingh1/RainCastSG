"""Download module for official Singapore Historical Rainfall datasets from data.gov.sg."""

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import requests
from tqdm import tqdm

from src.utils import get_logger, load_config

# Official Data.gov.sg dataset IDs for Collection 2279:
# https://data.gov.sg/collections/2279/view
DATASET_IDS: Dict[int, str] = {
    2017: "d_1990a5a1aeaf3dd243cf4dae294a61c4",
    2018: "d_024fb501ce7092b71bb713eaf54fa7eb",
    2019: "d_61995f092320e7155b7528050880b502",
    2020: "d_9e7de44094f876f6804b8b5bcee45c81",
    2021: "d_3b41598f74f1f11fc3430348fea51af5",
    2022: "d_42d64cc6c176ace1c52fbb40b9ede302",
    2023: "d_f864cc30d58b467db83659ad17c737bf",
    2024: "d_a0b69d3e02576a1fd0ab673e71f83507",
}

BASE_API_URL = "https://api-open.data.gov.sg/v1/public/api/datasets"


def fetch_download_url(dataset_id: str, max_retries: int = 15, retry_interval: int = 4) -> str:
    """Initiates download and polls until pre-signed S3 download URL is retrieved."""
    init_url = f"{BASE_API_URL}/{dataset_id}/initiate-download"
    poll_url = f"{BASE_API_URL}/{dataset_id}/poll-download"

    headers = {
        "User-Agent": "RainCastSG-Research/1.0 (NUS DSA5208)",
        "Accept": "application/json",
    }

    resp = requests.get(init_url, headers=headers, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Failed to initiate download for {dataset_id}: HTTP {resp.status_code} - {resp.text}")

    init_data = resp.json().get("data", {})
    if "url" in init_data and init_data["url"]:
        return init_data["url"]

    # Poll if URL was not immediately available
    for attempt in range(max_retries):
        time.sleep(retry_interval)
        poll_resp = requests.get(poll_url, headers=headers, timeout=30)
        if poll_resp.status_code == 200:
            poll_data = poll_resp.json().get("data", {})
            if "url" in poll_data and poll_data["url"]:
                return poll_data["url"]

    raise TimeoutError(f"Timed out waiting for download URL for dataset {dataset_id}")


def download_file(url: str, target_path: Path, chunk_size: int = 1048576) -> int:
    """Streams file download to target path with progress display."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_suffix(".tmp")

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        with open(temp_path, "wb") as f, tqdm(
            desc=target_path.name,
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    temp_path.replace(target_path)
    return target_path.stat().st_size


def download_datasets(
    years: List[int],
    raw_dir: str = "data/raw",
    force: bool = False
) -> Dict[str, Any]:
    """Downloads specified annual rainfall datasets from data.gov.sg."""
    logger = get_logger("RainCastSG.Download")
    raw_path = Path(raw_dir)
    manifest = {}

    for year in years:
        if year not in DATASET_IDS:
            logger.warning(f"Year {year} is not in registered dataset IDs. Skipping.")
            continue

        dataset_id = DATASET_IDS[year]
        target_file = raw_path / f"rainfall_{year}.csv"

        if target_file.exists() and not force and target_file.stat().st_size > 1000000:
            logger.info(f"Year {year} already downloaded at {target_file} ({target_file.stat().st_size:,} bytes). Skipping.")
            manifest[str(year)] = {
                "dataset_id": dataset_id,
                "file_path": str(target_file),
                "file_size_bytes": target_file.stat().st_size,
                "status": "cached"
            }
            continue

        logger.info(f"Initiating download for year {year} (Dataset ID: {dataset_id})...")
        try:
            url = fetch_download_url(dataset_id)
            size = download_file(url, target_file)
            logger.info(f"Successfully downloaded {target_file.name}: {size:,} bytes.")
            manifest[str(year)] = {
                "dataset_id": dataset_id,
                "file_path": str(target_file),
                "file_size_bytes": size,
                "status": "downloaded"
            }
        except Exception as e:
            logger.error(f"Failed to download year {year}: {e}")
            manifest[str(year)] = {
                "dataset_id": dataset_id,
                "error": str(e),
                "status": "failed"
            }

    # Save manifest
    manifest_path = Path("outputs/quality/download_manifest.json")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Download Singapore Rainfall Datasets from data.gov.sg")
    parser.add_argument("--years", nargs="+", type=int, help="Years to download, e.g. 2017 2018")
    parser.add_argument("--all", action="store_true", help="Download all years 2017 to 2024")
    parser.add_argument("--force", action="store_true", help="Force re-download even if files exist")
    args = parser.parse_args()

    config = load_config()
    all_years = config["data"]["years"]

    if args.all or not args.years:
        selected_years = all_years
    else:
        selected_years = args.years

    print(f"Downloading datasets for years: {selected_years}")
    download_datasets(selected_years, raw_dir=config["data"]["raw_dir"], force=args.force)


if __name__ == "__main__":
    main()
