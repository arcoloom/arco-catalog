from __future__ import annotations

from pathlib import Path
from urllib.request import Request, urlopen


SOURCE_URL = "https://instances.vantage.sh/instances.json"
DOWNLOAD_DIR = "downloads"
OUTPUT_NAME = "instances.json"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def download_instances_json() -> Path:
    download_dir = Path.cwd() / DOWNLOAD_DIR
    download_dir.mkdir(parents=True, exist_ok=True)

    output_path = download_dir / OUTPUT_NAME
    request = Request(SOURCE_URL, headers={"User-Agent": USER_AGENT})
    with urlopen(request) as response:
        output_path.write_bytes(response.read())

    return output_path


def main() -> None:
    output_path = download_instances_json()
    print(f"Downloaded {SOURCE_URL} -> {output_path}")


if __name__ == "__main__":
    main()
