from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SERIES_MODELS_FILENAME = "series_models.json"
INSTANCE_METADATA_FILENAME = "instance_metadata.json"
INSTANCE_REGIONS_FILENAME = "instance_regions.json"
DATASET_FILENAMES = (
    SERIES_MODELS_FILENAME,
    INSTANCE_METADATA_FILENAME,
    INSTANCE_REGIONS_FILENAME,
)


@dataclass(frozen=True)
class CatalogValidationConfig:
    min_raw_instances: int = 300
    min_matched_instances: int = 300
    min_match_ratio: float = 0.80
    min_series_count: int = 15
    min_region_rows: int = 12000
    min_spot_region_rows: int = 12000
    max_missing_region_name_ratio: float = 0.02
    min_metadata_baseline_ratio: float = 0.80
    min_series_baseline_ratio: float = 0.80
    min_regions_baseline_ratio: float = 0.80


DEFAULT_VALIDATION_CONFIG = CatalogValidationConfig()


@dataclass(frozen=True)
class CatalogSummary:
    raw_instances: int | None
    matched_instances: int
    series_count: int
    region_rows: int
    spot_region_rows: int
    missing_region_name_rows: int

    @property
    def match_ratio(self) -> float:
        if self.raw_instances is None or self.raw_instances <= 0:
            return 0.0
        return self.matched_instances / self.raw_instances

    @property
    def missing_region_name_ratio(self) -> float:
        if self.region_rows <= 0:
            return 0.0
        return self.missing_region_name_rows / self.region_rows


def load_dataset_payloads(dataset_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        _load_json_list(dataset_dir / SERIES_MODELS_FILENAME, SERIES_MODELS_FILENAME),
        _load_json_list(dataset_dir / INSTANCE_METADATA_FILENAME, INSTANCE_METADATA_FILENAME),
        _load_json_list(dataset_dir / INSTANCE_REGIONS_FILENAME, INSTANCE_REGIONS_FILENAME),
    )


def summarize_catalog(
    raw_instances: int | None,
    instance_metadata: list[dict[str, Any]],
    instance_regions: list[dict[str, Any]],
    series_models: list[dict[str, Any]],
) -> CatalogSummary:
    spot_region_rows = sum(1 for row in instance_regions if row.get("spot_price"))
    missing_region_name_rows = sum(1 for row in instance_regions if not row.get("region_name"))
    return CatalogSummary(
        raw_instances=raw_instances,
        matched_instances=len(instance_metadata),
        series_count=len(series_models),
        region_rows=len(instance_regions),
        spot_region_rows=spot_region_rows,
        missing_region_name_rows=missing_region_name_rows,
    )


def validate_catalog_payloads(
    *,
    raw_instances: int | None,
    series_models: list[dict[str, Any]],
    instance_metadata: list[dict[str, Any]],
    instance_regions: list[dict[str, Any]],
    baseline_dir: Path | None = None,
    config: CatalogValidationConfig = DEFAULT_VALIDATION_CONFIG,
) -> CatalogSummary:
    summary = summarize_catalog(
        raw_instances=raw_instances,
        instance_metadata=instance_metadata,
        instance_regions=instance_regions,
        series_models=series_models,
    )
    _validate_summary(summary, config)
    _validate_series_models(series_models)
    _validate_instance_metadata(instance_metadata)
    _validate_instance_regions(instance_regions, instance_metadata)
    _validate_cross_file_consistency(series_models, instance_metadata)

    baseline_summary = None
    if baseline_dir is not None:
        baseline_summary = load_existing_summary(baseline_dir)
    if baseline_summary is not None:
        _validate_against_baseline(summary, baseline_summary, config)

    return summary


def validate_dataset_dir(
    dataset_dir: Path,
    *,
    raw_instances: int | None = None,
    baseline_dir: Path | None = None,
    config: CatalogValidationConfig = DEFAULT_VALIDATION_CONFIG,
) -> CatalogSummary:
    series_models, instance_metadata, instance_regions = load_dataset_payloads(dataset_dir)
    return validate_catalog_payloads(
        raw_instances=raw_instances,
        series_models=series_models,
        instance_metadata=instance_metadata,
        instance_regions=instance_regions,
        baseline_dir=baseline_dir,
        config=config,
    )


def load_existing_summary(dataset_dir: Path) -> CatalogSummary | None:
    if not all((dataset_dir / filename).is_file() for filename in DATASET_FILENAMES):
        return None

    series_models, instance_metadata, instance_regions = load_dataset_payloads(dataset_dir)
    return summarize_catalog(
        raw_instances=None,
        instance_metadata=instance_metadata,
        instance_regions=instance_regions,
        series_models=series_models,
    )


def _load_json_list(path: Path, label: str) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"required dataset file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {exc}") from exc

    if not isinstance(payload, list):
        raise ValueError(f"{label} must be a JSON list")
    if not all(isinstance(item, dict) for item in payload):
        raise ValueError(f"{label} must contain only JSON objects")
    return payload


def _validate_summary(summary: CatalogSummary, config: CatalogValidationConfig) -> None:
    if summary.raw_instances is not None and summary.raw_instances < config.min_raw_instances:
        raise ValueError(
            f"raw instance count {summary.raw_instances} is below minimum {config.min_raw_instances}"
        )
    if summary.matched_instances < config.min_matched_instances:
        raise ValueError(
            f"matched instance count {summary.matched_instances} is below minimum {config.min_matched_instances}"
        )
    if summary.raw_instances is not None and summary.match_ratio < config.min_match_ratio:
        raise ValueError(
            f"matched instance ratio {summary.match_ratio:.2%} is below minimum {config.min_match_ratio:.2%}"
        )
    if summary.series_count < config.min_series_count:
        raise ValueError(
            f"series count {summary.series_count} is below minimum {config.min_series_count}"
        )
    if summary.region_rows < config.min_region_rows:
        raise ValueError(
            f"region row count {summary.region_rows} is below minimum {config.min_region_rows}"
        )
    if summary.spot_region_rows < config.min_spot_region_rows:
        raise ValueError(
            f"spot region row count {summary.spot_region_rows} is below minimum {config.min_spot_region_rows}"
        )
    if summary.missing_region_name_ratio > config.max_missing_region_name_ratio:
        raise ValueError(
            "region_name missing ratio "
            f"{summary.missing_region_name_ratio:.2%} exceeds maximum {config.max_missing_region_name_ratio:.2%}"
        )


def _validate_series_models(series_models: list[dict[str, Any]]) -> None:
    seen_series: set[str] = set()
    for row in series_models:
        series = str(row.get("series", "")).strip()
        if not series:
            raise ValueError("series_models contains an empty series value")
        if series in seen_series:
            raise ValueError(f"series_models contains duplicate series {series!r}")
        seen_series.add(series)

        instance_types = row.get("instance_types")
        if not isinstance(instance_types, list) or not instance_types:
            raise ValueError(f"series_models[{series!r}] is missing instance_types")
        if not all(isinstance(item, str) and item.strip() for item in instance_types):
            raise ValueError(f"series_models[{series!r}] has invalid instance_types entries")
        if len(set(instance_types)) != len(instance_types):
            raise ValueError(f"series_models[{series!r}] contains duplicate instance_types")

        instance_count = row.get("instance_count")
        if instance_count != len(instance_types):
            raise ValueError(
                f"series_models[{series!r}] instance_count {instance_count!r} "
                f"does not match {len(instance_types)} instance_types"
            )


def _validate_instance_metadata(instance_metadata: list[dict[str, Any]]) -> None:
    seen_instance_types: set[str] = set()
    for row in instance_metadata:
        instance_type = str(row.get("instance_type", "")).strip()
        if not instance_type:
            raise ValueError("instance_metadata contains an empty instance_type")
        if instance_type in seen_instance_types:
            raise ValueError(f"instance_metadata contains duplicate instance_type {instance_type!r}")
        seen_instance_types.add(instance_type)

        series = str(row.get("series", "")).strip()
        if not series:
            raise ValueError(f"instance_metadata[{instance_type!r}] is missing series")

        support_os = row.get("support_os")
        if support_os is None or not isinstance(support_os, list):
            raise ValueError(f"instance_metadata[{instance_type!r}] has invalid support_os")

        arch = row.get("arch")
        if arch is None or not isinstance(arch, list) or not arch:
            raise ValueError(f"instance_metadata[{instance_type!r}] has invalid arch")


def _validate_instance_regions(
    instance_regions: list[dict[str, Any]],
    instance_metadata: list[dict[str, Any]],
) -> None:
    metadata_instance_types = {
        str(row.get("instance_type", "")).strip()
        for row in instance_metadata
    }
    seen_pairs: set[tuple[str, str]] = set()
    for row in instance_regions:
        instance_type = str(row.get("instance_type", "")).strip()
        region_code = str(row.get("region_code", "")).strip()
        if not instance_type:
            raise ValueError("instance_regions contains an empty instance_type")
        if instance_type not in metadata_instance_types:
            raise ValueError(f"instance_regions references unknown instance_type {instance_type!r}")
        if not region_code:
            raise ValueError(f"instance_regions[{instance_type!r}] is missing region_code")

        pair = (instance_type, region_code)
        if pair in seen_pairs:
            raise ValueError(
                f"instance_regions contains duplicate instance_type/region_code pair {pair!r}"
            )
        seen_pairs.add(pair)


def _validate_cross_file_consistency(
    series_models: list[dict[str, Any]],
    instance_metadata: list[dict[str, Any]],
) -> None:
    metadata_instance_types = {
        str(row.get("instance_type", "")).strip()
        for row in instance_metadata
    }
    metadata_series = {
        str(row.get("series", "")).strip()
        for row in instance_metadata
    }
    series_model_series = {
        str(row.get("series", "")).strip()
        for row in series_models
    }
    if metadata_series != series_model_series:
        raise ValueError("series_models series set does not match instance_metadata series set")

    for row in series_models:
        series = str(row.get("series", "")).strip()
        for instance_type in row.get("instance_types", []):
            if instance_type not in metadata_instance_types:
                raise ValueError(
                    f"series_models[{series!r}] references unknown instance_type {instance_type!r}"
                )


def _validate_against_baseline(
    summary: CatalogSummary,
    baseline: CatalogSummary,
    config: CatalogValidationConfig,
) -> None:
    _validate_floor(
        "matched instance count",
        summary.matched_instances,
        baseline.matched_instances,
        config.min_metadata_baseline_ratio,
    )
    _validate_floor(
        "series count",
        summary.series_count,
        baseline.series_count,
        config.min_series_baseline_ratio,
    )
    _validate_floor(
        "region row count",
        summary.region_rows,
        baseline.region_rows,
        config.min_regions_baseline_ratio,
    )
def _validate_floor(label: str, current: int, previous: int, ratio: float) -> None:
    if previous <= 0:
        return
    required = max(1, math.ceil(previous * ratio))
    if current < required:
        raise ValueError(
            f"{label} {current} dropped below {ratio:.0%} of previous snapshot {previous}"
        )
