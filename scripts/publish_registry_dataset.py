from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import subprocess
import tempfile
from pathlib import Path


DEFAULT_BUCKET = "arco-registry"
DEFAULT_BASE_URL = "https://registry.arcoloom.com"
DEFAULT_CHANNELS = ("latest", "stable")
DATASET_PROVIDER = "aws"
DATASET_NAME = "ec2"
DATASET_FILES = {
    "instance_metadata": "instance_metadata.json",
    "instance_regions": "instance_regions.json",
    "series_models": "series_models.json",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish arco-catalog AWS dataset files and manifests to the registry bucket."
    )
    parser.add_argument("--version", required=True, help="Immutable dataset version.")
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=Path("data/aws"),
        help="Directory containing normalized AWS dataset JSON files.",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("ARCO_REGISTRY_BUCKET", DEFAULT_BUCKET),
        help="Registry R2 bucket name.",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("ARCO_REGISTRY_BASE_URL", DEFAULT_BASE_URL),
        help="Public registry base URL.",
    )
    parser.add_argument(
        "--channel",
        dest="channels",
        action="append",
        default=[],
        help="Channel name to update. Can be provided multiple times.",
    )
    return parser


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def upload_object(bucket: str, key: str, source: Path) -> None:
    subprocess.run(
        [
            "npx",
            "wrangler@4",
            "r2",
            "object",
            "put",
            f"{bucket}/{key}",
            "--file",
            str(source),
        ],
        check=True,
    )


def write_json_temp(payload: dict[str, object]) -> Path:
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False)
    with handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return Path(handle.name)


def content_type_for(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(path.name)
    return guessed or "application/octet-stream"


def main() -> None:
    args = build_parser().parse_args()
    version = args.version.strip()
    if not version:
        raise SystemExit("--version is required")

    channels = tuple(dict.fromkeys(args.channels or DEFAULT_CHANNELS))
    base_url = args.base_url.rstrip("/")
    dataset_dir: Path = args.dataset_dir

    files: dict[str, dict[str, object]] = {}
    for alias, filename in DATASET_FILES.items():
        path = dataset_dir / filename
        if not path.is_file():
            raise SystemExit(f"dataset file not found: {path}")
        key = f"datasets/{DATASET_PROVIDER}/{DATASET_NAME}/{version}/{filename}"
        upload_object(args.bucket, key, path)
        files[alias] = {
            "key": key,
            "filename": filename,
            "sha256": sha256_file(path),
            "size": path.stat().st_size,
            "content_type": content_type_for(path),
        }

    version_manifest = {
        "schema": "arco.dataset.version.v1",
        "dataset": {
            "provider": DATASET_PROVIDER,
            "name": DATASET_NAME,
            "version": version,
        },
        "files": files,
        "metadata": {
            "source": "arco-catalog",
        },
    }
    version_manifest_path = write_json_temp(version_manifest)
    try:
        upload_object(
            args.bucket,
            f"manifests/versions/datasets/{DATASET_PROVIDER}/{DATASET_NAME}/{version}.json",
            version_manifest_path,
        )
    finally:
        version_manifest_path.unlink(missing_ok=True)

    for channel in channels:
        channel_manifest = {
            "schema": "arco.dataset.channel.v1",
            "channel": channel,
            "dataset": {
                "provider": DATASET_PROVIDER,
                "name": DATASET_NAME,
                "version": version,
            },
            "files": files,
            "metadata": {
                "source": "arco-catalog",
                "channel": channel,
                "base_url": base_url,
            },
        }
        channel_manifest_path = write_json_temp(channel_manifest)
        try:
            upload_object(
                args.bucket,
                f"manifests/channels/datasets/{DATASET_PROVIDER}/{DATASET_NAME}/{channel}.json",
                channel_manifest_path,
            )
        finally:
            channel_manifest_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
