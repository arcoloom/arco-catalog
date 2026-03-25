from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import subprocess
import tempfile
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.aws.catalog_validation import validate_dataset_dir as validate_aws_dataset_dir
from scripts.gcp.catalog_validation import validate_dataset_dir as validate_gcp_dataset_dir


DEFAULT_BUCKET = "arco-registry"
DEFAULT_BASE_URL = "https://registry.arcoloom.com"
DEFAULT_CHANNELS = ("latest", "stable")
DEFAULT_DATASET_FILES = {
    "instance_metadata": "instance_metadata.json",
    "instance_regions": "instance_regions.json",
    "series_models": "series_models.json",
}

DATASET_VALIDATORS = {
    ("aws", "ec2"): validate_aws_dataset_dir,
    ("gcp", "compute"): validate_gcp_dataset_dir,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish arco-catalog dataset files and manifests to the registry bucket."
    )
    parser.add_argument("--version", required=True, help="Immutable dataset version.")
    parser.add_argument(
        "--provider",
        default="aws",
        help="Dataset provider namespace. Example: aws, gcp.",
    )
    parser.add_argument(
        "--dataset",
        default="ec2",
        help="Dataset name within the provider namespace. Example: ec2, compute.",
    )
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
    provider = args.provider.strip().lower()
    dataset_name = args.dataset.strip().lower()
    if not provider:
        raise SystemExit("--provider is required")
    if not dataset_name:
        raise SystemExit("--dataset is required")

    channels = tuple(dict.fromkeys(args.channels or DEFAULT_CHANNELS))
    base_url = args.base_url.rstrip("/")
    dataset_dir: Path = args.dataset_dir

    validator = DATASET_VALIDATORS.get((provider, dataset_name))
    if validator is None:
        raise SystemExit(f"unsupported dataset validator for {provider}/{dataset_name}")

    validator(dataset_dir)

    files: dict[str, dict[str, object]] = {}
    for alias, filename in DEFAULT_DATASET_FILES.items():
        path = dataset_dir / filename
        if not path.is_file():
            raise SystemExit(f"dataset file not found: {path}")
        key = f"datasets/{provider}/{dataset_name}/{version}/{filename}"
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
            "provider": provider,
            "name": dataset_name,
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
            f"manifests/versions/datasets/{provider}/{dataset_name}/{version}.json",
            version_manifest_path,
        )
    finally:
        version_manifest_path.unlink(missing_ok=True)

    for channel in channels:
        channel_manifest = {
            "schema": "arco.dataset.channel.v1",
            "channel": channel,
            "dataset": {
                "provider": provider,
                "name": dataset_name,
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
                f"manifests/channels/datasets/{provider}/{dataset_name}/{channel}.json",
                channel_manifest_path,
            )
        finally:
            channel_manifest_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
