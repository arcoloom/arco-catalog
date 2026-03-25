from __future__ import annotations

from pathlib import Path
from urllib.request import Request, urlopen


SOURCE_URL = "https://instances.vantage.sh/gcp/instances.json"
DOWNLOAD_DIR = "downloads"
OUTPUT_NAME = "instances.json"
DOWNLOAD_TIMEOUT_SECONDS = 60
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def download_instances_json(
    download_dir: Path | None = None,
    source_url: str = SOURCE_URL,
) -> Path:
    download_dir = (
        Path(download_dir)
        if download_dir is not None
        else Path.cwd() / DOWNLOAD_DIR
    )
    download_dir.mkdir(parents=True, exist_ok=True)

    output_path = download_dir / OUTPUT_NAME
    request = Request(source_url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
        payload = response.read()

    if not payload.strip():
        raise ValueError(f"downloaded payload from {source_url} is empty")

    if payload.lstrip()[:1] != b"[":
        raise ValueError(
            f"downloaded payload from {source_url} does not look like a JSON array"
        )

    output_path.write_bytes(payload)

    return output_path


def main() -> None:
    output_path = download_instances_json()
    print(f"Downloaded {SOURCE_URL} -> {output_path}")


if __name__ == "__main__":
    main()
