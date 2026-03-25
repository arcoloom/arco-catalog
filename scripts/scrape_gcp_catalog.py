from __future__ import annotations

import argparse
import tempfile
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.gcp.catalog_validation import (
    DEFAULT_VALIDATION_CONFIG,
    INSTANCE_METADATA_FILENAME,
    INSTANCE_REGIONS_FILENAME,
    SERIES_MODELS_FILENAME,
    validate_catalog_payloads,
)
from scripts.gcp.clean_instance_catalog import (
    DEFAULT_OUTPUT_DIR,
    build_instance_metadata,
    build_instance_regions,
    build_series_models,
    enrich_instances,
    load_instances,
    write_json,
)
from scripts.gcp.download import download_instances_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download the latest GCP instance catalog and refresh the "
            "normalized Arcoloom metadata files."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Reuse an existing instances.json file instead of downloading a fresh copy.",
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
    enriched_instances = enrich_instances(instances)

    series_models = build_series_models(enriched_instances)
    instance_metadata = build_instance_metadata(enriched_instances)
    instance_regions = build_instance_regions(enriched_instances)

    validate_catalog_payloads(
        raw_instances=len(instances),
        series_models=series_models,
        instance_metadata=instance_metadata,
        instance_regions=instance_regions,
        baseline_dir=output_dir if output_dir.exists() else None,
        config=DEFAULT_VALIDATION_CONFIG,
    )

    with tempfile.TemporaryDirectory(prefix="arco-gcp-catalog-") as temp_dir_name:
        temp_output_dir = Path(temp_dir_name)
        write_json(temp_output_dir / SERIES_MODELS_FILENAME, series_models)
        write_json(temp_output_dir / INSTANCE_METADATA_FILENAME, instance_metadata)
        write_json(temp_output_dir / INSTANCE_REGIONS_FILENAME, instance_regions)

        output_dir.mkdir(parents=True, exist_ok=True)
        for filename in (
            SERIES_MODELS_FILENAME,
            INSTANCE_METADATA_FILENAME,
            INSTANCE_REGIONS_FILENAME,
        ):
            (temp_output_dir / filename).replace(output_dir / filename)

    return source_path, len(enriched_instances)


def main() -> None:
    args = build_parser().parse_args()
    try:
        source_path, matched_instances = refresh_catalog(args.input, args.output_dir)
    except (OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    output_dir = args.output_dir
    print(f"Source: {source_path}")
    print(f"Matched instances: {matched_instances}")
    print(f"Series/models: {output_dir / SERIES_MODELS_FILENAME}")
    print(f"Instance metadata: {output_dir / INSTANCE_METADATA_FILENAME}")
    print(f"Instance regions: {output_dir / INSTANCE_REGIONS_FILENAME}")


if __name__ == "__main__":
    main()
