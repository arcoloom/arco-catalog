from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.aws.catalog_validation import (
    CatalogValidationConfig,
    SERIES_MODELS_FILENAME,
    INSTANCE_METADATA_FILENAME,
    INSTANCE_REGIONS_FILENAME,
    validate_catalog_payloads,
    validate_dataset_dir,
)


def build_sample_payloads() -> tuple[list[dict], list[dict], list[dict]]:
    series_models = [
        {
            "series": "m7i",
            "instance_count": 2,
            "instance_types": ["m7i.large", "m7i.xlarge"],
        },
        {
            "series": "c7g",
            "instance_count": 1,
            "instance_types": ["c7g.large"],
        },
    ]
    instance_metadata = [
        {
            "series": "m7i",
            "instance_type": "m7i.large",
            "support_os": ["linux"],
        },
        {
            "series": "m7i",
            "instance_type": "m7i.xlarge",
            "support_os": ["linux", "windows"],
        },
        {
            "series": "c7g",
            "instance_type": "c7g.large",
            "support_os": ["linux"],
        },
    ]
    instance_regions = [
        {
            "series": "m7i",
            "instance_type": "m7i.large",
            "region_code": "us-east-1",
            "region_name": "US East (N. Virginia)",
            "on_demand_price": "0.10",
        },
        {
            "series": "m7i",
            "instance_type": "m7i.xlarge",
            "region_code": "us-east-1",
            "region_name": "US East (N. Virginia)",
            "on_demand_price": "0.20",
        },
        {
            "series": "c7g",
            "instance_type": "c7g.large",
            "region_code": "us-west-2",
            "region_name": "US West (Oregon)",
            "on_demand_price": "0.30",
        },
    ]
    return series_models, instance_metadata, instance_regions


class CatalogValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = CatalogValidationConfig(
            min_raw_instances=3,
            min_matched_instances=3,
            min_match_ratio=0.9,
            min_series_count=2,
            min_region_rows=3,
            min_priced_region_rows=3,
            max_missing_region_name_ratio=0.0,
            min_metadata_baseline_ratio=0.8,
            min_series_baseline_ratio=0.8,
            min_regions_baseline_ratio=0.8,
            min_priced_regions_baseline_ratio=0.8,
        )

    def test_validate_catalog_payloads_accepts_consistent_data(self) -> None:
        series_models, instance_metadata, instance_regions = build_sample_payloads()
        summary = validate_catalog_payloads(
            raw_instances=3,
            series_models=series_models,
            instance_metadata=instance_metadata,
            instance_regions=instance_regions,
            config=self.config,
        )

        self.assertEqual(summary.matched_instances, 3)
        self.assertEqual(summary.region_rows, 3)
        self.assertEqual(summary.priced_region_rows, 3)

    def test_validate_catalog_payloads_rejects_large_baseline_drop(self) -> None:
        series_models, instance_metadata, instance_regions = build_sample_payloads()
        with tempfile.TemporaryDirectory() as temp_dir_name:
            baseline_dir = Path(temp_dir_name)
            write_payload(baseline_dir / SERIES_MODELS_FILENAME, series_models)
            write_payload(baseline_dir / INSTANCE_METADATA_FILENAME, instance_metadata)
            write_payload(baseline_dir / INSTANCE_REGIONS_FILENAME, instance_regions)

            with self.assertRaisesRegex(ValueError, "matched instance count"):
                validate_catalog_payloads(
                    raw_instances=3,
                    series_models=series_models,
                    instance_metadata=instance_metadata[:2],
                    instance_regions=instance_regions[:2],
                    baseline_dir=baseline_dir,
                    config=self.config,
                )

    def test_validate_dataset_dir_rejects_missing_region_names(self) -> None:
        series_models, instance_metadata, instance_regions = build_sample_payloads()
        instance_regions[0]["region_name"] = None

        with tempfile.TemporaryDirectory() as temp_dir_name:
            dataset_dir = Path(temp_dir_name)
            write_payload(dataset_dir / SERIES_MODELS_FILENAME, series_models)
            write_payload(dataset_dir / INSTANCE_METADATA_FILENAME, instance_metadata)
            write_payload(dataset_dir / INSTANCE_REGIONS_FILENAME, instance_regions)

            with self.assertRaisesRegex(ValueError, "region_name missing ratio"):
                validate_dataset_dir(
                    dataset_dir,
                    raw_instances=3,
                    config=self.config,
                )


def write_payload(path: Path, payload: list[dict]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
