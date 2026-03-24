#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


DEFAULT_INPUT = Path.cwd() / "downloads" / "instances.json"
DEFAULT_OUTPUT_DIR = Path.cwd() / "data" / "aws"

METADATA_FIELDS = [
    "series",
    "instance_type",
    "family",
    "pretty_name",
    "generation",
    "vCPU",
    "memory",
    "memory_speed",
    "clock_speed_ghz",
    "physical_processor",
    "arch",
    "network_performance",
    "enhanced_networking",
    "vpc_only",
    "ipv6_support",
    "placement_group_support",
    "vpc",
    "ebs_optimized",
    "ebs_as_nvme",
    "ebs_baseline_throughput",
    "ebs_baseline_iops",
    "ebs_baseline_bandwidth",
    "ebs_throughput",
    "ebs_iops",
    "ebs_max_bandwidth",
    "storage",
    "GPU",
    "GPU_model",
    "GPU_memory",
    "FPGA",
    "compute_capability",
    "intel_avx",
    "intel_avx2",
    "intel_avx512",
    "intel_turbo",
    "linux_virtualization_types",
    "emr",
    "availability_zones",
    "coremark_iterations_second",
    "ffmpeg_speed",
    "ffmpeg_fps",
    "ffmpeg_used_cuda",
    "base_performance",
    "burst_minutes",
    "uses_numa_architecture",
]

CATEGORICAL_NETWORK_PERFORMANCE = {
    "Very Low": "Very Low",
    "Low": "Low",
    "Low to Moderate": "Low to Moderate",
    "Moderate": "Moderate",
    "High": "High",
}


def extract_series(instance_type: str) -> str | None:
    match = re.match(r"^([a-z]+\d+[a-z]*)", instance_type.strip().lower())
    return match.group(1) if match else None


def load_instances(input_path: Path) -> list[dict[str, Any]]:
    if not input_path.exists():
        print(f"Error: file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Error: failed to parse JSON: {exc}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(payload, list):
        print(
            f"Error: expected top-level JSON to be a list, got {type(payload).__name__}",
            file=sys.stderr,
        )
        sys.exit(1)

    return [item for item in payload if isinstance(item, dict)]


def enrich_instances_with_series(instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        enriched_instances.append(enriched)

    return sorted(enriched_instances, key=lambda item: item["instance_type"])


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


def normalize_network_performance(raw_value: Any) -> Any:
    if raw_value is None:
        return None

    value = str(raw_value).strip()
    if not value:
        return None

    if value in CATEGORICAL_NETWORK_PERFORMANCE:
        return CATEGORICAL_NETWORK_PERFORMANCE[value]

    up_to_match = re.fullmatch(r"Up to (\d+(?:\.\d+)?) Gigabit", value)
    if up_to_match:
        return f"<={up_to_match.group(1)}G"

    direct_match = re.fullmatch(r"(\d+(?:\.\d+)?) Gigabit", value)
    if direct_match:
        return f"{direct_match.group(1)}G"

    multi_link_match = re.fullmatch(r"(\d+)x (\d+(?:\.\d+)?) Gigabit", value)
    if multi_link_match:
        return f"{multi_link_match.group(1)}x{multi_link_match.group(2)}G"

    raise ValueError(f"Unsupported network_performance value: {value}")


def build_instance_metadata(instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in instances:
        metadata = {field: item.get(field) for field in METADATA_FIELDS}
        metadata["network_performance"] = normalize_network_performance(
            metadata["network_performance"]
        )
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
            on_demand_price = None
            region_pricing = pricing.get(region_code)
            if isinstance(region_pricing, dict):
                linux_pricing = region_pricing.get("linux")
                if isinstance(linux_pricing, dict):
                    raw_on_demand = linux_pricing.get("ondemand")
                    if raw_on_demand is not None:
                        value = str(raw_on_demand).strip()
                        if value:
                            on_demand_price = value

            rows.append(
                {
                    "series": series,
                    "instance_type": instance_type,
                    "region_code": region_code,
                    "region_name": regions.get(region_code),
                    "on_demand_price": on_demand_price,
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
            "Clean AWS EC2 instance catalog data and output "
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
    instances = load_instances(args.input)
    instances_with_series = enrich_instances_with_series(instances)

    try:
        series_models = build_series_models(instances_with_series)
        instance_metadata = build_instance_metadata(instances_with_series)
        instance_regions = build_instance_regions(instances_with_series)
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

    print(f"Matched instances: {len(instances_with_series)}")
    print(f"Series/models: {series_models_path}")
    print(f"Instance metadata: {instance_metadata_path}")
    print(f"Instance regions: {instance_regions_path}")


if __name__ == "__main__":
    main()
