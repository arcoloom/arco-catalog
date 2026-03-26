#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


DEFAULT_INPUT = Path.cwd() / "downloads" / "instances.json"
DEFAULT_OUTPUT_DIR = Path.cwd() / "data" / "gcp"

METADATA_FIELDS = [
    "series",
    "instance_type",
    "family",
    "pretty_name",
    "generation",
    "vCPU",
    "memory",
    "network_performance",
    "GPU",
    "GPU_model",
    "local_ssd",
    "shared_cpu",
    "arch",
]

ARM_SERIES = {"t2a", "c4a", "n4a", "a4x"}
GPU_MODEL_NORMALIZATION = {
    "nvidia-a100-80gb": "NVIDIA A100",
    "nvidia-tesla-a100": "NVIDIA A100",
    "nvidia-h100-80gb": "NVIDIA H100",
    "nvidia-h100-mega-80gb": "NVIDIA H100",
    "nvidia-h200-141gb": "NVIDIA H200",
    "nvidia-l4": "NVIDIA L4",
}


def extract_series(instance_type: str) -> str | None:
    match = re.match(r"^([a-z]+\d+[a-z]*)", instance_type.strip().lower())
    return match.group(1) if match else None


def normalize_architectures(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        item = str(value).strip().lower()
        if not item:
            continue
        if item in {"amd64", "x86_64"}:
            canonical = "x86_64"
        elif item in {"arm64", "aarch64"}:
            canonical = "arm64"
        else:
            canonical = value

        if canonical not in normalized:
            normalized.append(canonical)
    return normalized


def extract_architectures(instance_type: str) -> list[str]:
    series = extract_series(instance_type or "")
    if series in ARM_SERIES:
        return normalize_architectures(["arm64"])
    return normalize_architectures(["amd64"])


def load_instances(input_path: Path) -> list[dict[str, Any]]:
    if not input_path.exists():
        raise ValueError(f"file not found: {input_path}")

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"failed to parse JSON: {exc}") from exc

    if not isinstance(payload, list):
        raise ValueError(
            f"expected top-level JSON to be a list, got {type(payload).__name__}"
        )

    if not payload:
        raise ValueError("source payload is empty")
    if not all(isinstance(item, dict) for item in payload):
        raise ValueError("source payload must contain only JSON objects")

    return payload


def enrich_instances(instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched_instances: list[dict[str, Any]] = []
    for item in instances:
        instance_type = str(item.get("instance_type", "")).strip()
        if not instance_type:
            continue

        series = extract_series(instance_type)
        if not series:
            continue

        enriched = dict(item)
        enriched["series"] = series
        enriched["arch"] = extract_architectures(instance_type)
        enriched_instances.append(enriched)

    return sorted(enriched_instances, key=lambda item: item["instance_type"])


def extract_support_os(item: dict[str, Any]) -> list[str]:
    pricing = item.get("pricing") or {}
    if not isinstance(pricing, dict):
        return []

    os_names: set[str] = set()
    for region_pricing in pricing.values():
        if not isinstance(region_pricing, dict):
            continue
        for os_name, os_value in region_pricing.items():
            if not isinstance(os_value, dict):
                continue
            if not os_value:
                continue
            os_names.add(os_name)
    return sorted(os_names)


def normalize_gpu_model(raw_value: Any) -> Any:
    if raw_value is None:
        return None

    value = str(raw_value).strip()
    if not value:
        return None

    normalized = GPU_MODEL_NORMALIZATION.get(value.lower())
    if normalized is not None:
        return normalized

    return value


def build_series_models(instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[str]] = {}
    for item in instances:
        series = item["series"]
        grouped.setdefault(series, []).append(item["instance_type"])

    return [
        {
            "series": series,
            "instance_count": len(sorted(set(instance_types))),
            "instance_types": sorted(set(instance_types)),
        }
        for series, instance_types in sorted(grouped.items())
    ]


def build_instance_metadata(instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in instances:
        metadata = {field: item.get(field) for field in METADATA_FIELDS}
        metadata["GPU_model"] = normalize_gpu_model(metadata.get("GPU_model"))
        metadata["support_os"] = extract_support_os(item)
        rows.append(metadata)
    return rows


def build_instance_regions(instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in instances:
        instance_type = item["instance_type"]
        series = item["series"]

        regions = item.get("regions") or {}
        if not isinstance(regions, dict):
            regions = {}

        pricing = item.get("pricing") or {}
        if not isinstance(pricing, dict):
            pricing = {}

        region_codes = sorted(set(regions.keys()) | set(pricing.keys()))
        for region_code in region_codes:
            region_pricing = pricing.get(region_code)
            spot_price = None
            if isinstance(region_pricing, dict):
                linux_pricing = region_pricing.get("linux")
                if isinstance(linux_pricing, dict):
                    raw_spot = linux_pricing.get("spot")
                    if raw_spot is not None:
                        value = str(raw_spot).strip()
                        if value:
                            spot_price = value

            rows.append(
                {
                    "series": series,
                    "instance_type": instance_type,
                    "region_code": region_code,
                    "region_name": regions.get(region_code),
                    "spot_price": spot_price,
                }
            )

    return rows


def write_json(output_path: Path, payload: Any) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Clean GCP instance catalog data and output "
            "series/models, instance metadata, and region availability."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input instances.json path. Default: {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        instances = load_instances(args.input)
        enriched_instances = enrich_instances(instances)

        series_models = build_series_models(enriched_instances)
        instance_metadata = build_instance_metadata(enriched_instances)
        instance_regions = build_instance_regions(enriched_instances)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    output_dir = args.output_dir
    series_models_path = output_dir / "series_models.json"
    instance_metadata_path = output_dir / "instance_metadata.json"
    instance_regions_path = output_dir / "instance_regions.json"

    write_json(series_models_path, series_models)
    write_json(instance_metadata_path, instance_metadata)
    write_json(instance_regions_path, instance_regions)

    print(f"Matched instances: {len(enriched_instances)}")
    print(f"Series/models: {series_models_path}")
    print(f"Instance metadata: {instance_metadata_path}")
    print(f"Instance regions: {instance_regions_path}")


if __name__ == "__main__":
    main()
