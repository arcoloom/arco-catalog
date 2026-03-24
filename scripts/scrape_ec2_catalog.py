from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.aws.clean_instance_catalog import (
    DEFAULT_OUTPUT_DIR,
    build_instance_metadata,
    build_instance_regions,
    build_series_models,
    enrich_instances_with_series,
    load_instances,
    write_json,
)
from scripts.aws.download import download_instances_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download the latest AWS EC2 instance catalog and refresh the "
            "normalized Arcoloom metadata files."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        help=(
            "Reuse an existing instances.json file instead of downloading a "
            "fresh copy from the upstream source."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser


def refresh_catalog(input_path: Path | None, output_dir: Path) -> tuple[Path, int]:
    source_path = Path(input_path) if input_path is not None else download_instances_json()

    instances = load_instances(source_path)
    instances_with_series = enrich_instances_with_series(instances)

    series_models = build_series_models(instances_with_series)
    instance_metadata = build_instance_metadata(instances_with_series)
    instance_regions = build_instance_regions(instances_with_series)

    write_json(output_dir / "series_models.json", series_models)
    write_json(output_dir / "instance_metadata.json", instance_metadata)
    write_json(output_dir / "instance_regions.json", instance_regions)

    return source_path, len(instances_with_series)


def main() -> None:
    args = build_parser().parse_args()

    try:
        source_path, matched_instances = refresh_catalog(args.input, args.output_dir)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    output_dir = args.output_dir
    print(f"Source: {source_path}")
    print(f"Matched instances: {matched_instances}")
    print(f"Series/models: {output_dir / 'series_models.json'}")
    print(f"Instance metadata: {output_dir / 'instance_metadata.json'}")
    print(f"Instance regions: {output_dir / 'instance_regions.json'}")


if __name__ == "__main__":
    main()
