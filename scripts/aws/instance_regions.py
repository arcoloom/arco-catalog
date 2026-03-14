#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


DEFAULT_INPUT = Path.cwd() / "downloads" / "instances.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract the list of Regions where a specific AWS EC2 instance type is available."
    )
    parser.add_argument("instance_type", help="Instance type to look up, for example c6g.xlarge")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to the instance catalog JSON file. Default: {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format. Default: text",
    )
    return parser


def load_instances(input_path: Path) -> list[dict]:
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
            f"Error: expected the top-level JSON structure to be a list, got {type(payload).__name__}",
            file=sys.stderr,
        )
        sys.exit(1)

    return payload


def find_instance(instances: list[dict], instance_type: str) -> dict | None:
    normalized = instance_type.strip().lower()
    for item in instances:
        if str(item.get("instance_type", "")).lower() == normalized:
            return item
    return None


def build_result(instance: dict) -> dict:
    regions = instance.get("regions") or {}
    if not isinstance(regions, dict):
        regions = {}

    availability_zones = instance.get("availability_zones") or {}
    if not isinstance(availability_zones, dict):
        availability_zones = {}

    sorted_regions = [
        {"code": code, "name": name} for code, name in sorted(regions.items())
    ]

    return {
        "instance_type": instance.get("instance_type"),
        "region_count": len(sorted_regions),
        "regions": sorted_regions,
        "availability_zones_available": bool(availability_zones),
        "note": (
            "The current data source does not provide usable availability_zones, so this result only reflects Region-level availability."
            if not availability_zones
            else None
        ),
    }


def print_text(result: dict) -> None:
    print(f"Instance type: {result['instance_type']}")
    print(f"Available Regions: {result['region_count']}")
    if result["note"]:
        print(result["note"])
    print()

    for region in result["regions"]:
        print(f"{region['code']}\t{region['name']}")


def main() -> None:
    args = build_parser().parse_args()
    instances = load_instances(args.input)
    instance = find_instance(instances, args.instance_type)
    if instance is None:
        print(f"Error: instance type not found: {args.instance_type}", file=sys.stderr)
        sys.exit(1)

    result = build_result(instance)
    if args.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print_text(result)


if __name__ == "__main__":
    main()
