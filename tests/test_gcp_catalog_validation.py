from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.gcp.clean_instance_catalog import normalize_architectures, normalize_gpu_model
from scripts.gcp.catalog_validation import (
    CatalogValidationConfig,
    INSTANCE_METADATA_FILENAME,
    INSTANCE_REGIONS_FILENAME,
    SERIES_MODELS_FILENAME,
    validate_catalog_payloads,
    validate_dataset_dir,
)


def build_sample_payloads() -> tuple[list[dict], list[dict], list[dict]]:
    series_models = [
        {
            "series": "n2",
            "instance_count": 2,
            "instance_types": ["n2-standard-2", "n2-standard-4"],
        },
        {
            "series": "t2a",
            "instance_count": 1,
            "instance_types": ["t2a-standard-2"],
        },
    ]
    instance_metadata = [
        {
            "series": "n2",
            "instance_type": "n2-standard-2",
            "arch": ["x86_64"],
            "support_os": ["linux", "windows"],
        },
        {
            "series": "n2",
            "instance_type": "n2-standard-4",
            "arch": ["x86_64"],
            "support_os": ["linux", "windows"],
        },
        {
            "series": "t2a",
            "instance_type": "t2a-standard-2",
            "arch": ["arm64"],
            "support_os": ["linux", "windows"],
        },
    ]
    instance_regions = [
        {
            "series": "n2",
            "instance_type": "n2-standard-2",
            "region_code": "us-central1",
            "region_name": "Iowa",
            "spot_price": "0.02",
        },
        {
            "series": "n2",
            "instance_type": "n2-standard-4",
            "region_code": "us-central1",
            "region_name": "Iowa",
            "spot_price": "0.04",
        },
        {
            "series": "t2a",
            "instance_type": "t2a-standard-2",
            "region_code": "europe-west1",
            "region_name": "Belgium",
            "spot_price": "0.06",
        },
    ]
    return series_models, instance_metadata, instance_regions


class GCPCatalogValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = CatalogValidationConfig(
            min_raw_instances=3,
            min_matched_instances=3,
            min_match_ratio=0.9,
            min_series_count=2,
            min_region_rows=3,
            min_spot_region_rows=3,
            max_missing_region_name_ratio=0.0,
            min_metadata_baseline_ratio=0.8,
            min_series_baseline_ratio=0.8,
            min_regions_baseline_ratio=0.8,
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
        self.assertEqual(summary.spot_region_rows, 3)

    def test_validate_catalog_payloads_rejects_missing_spot_coverage(self) -> None:
        series_models, instance_metadata, instance_regions = build_sample_payloads()
        instance_regions[0]["spot_price"] = None
        with self.assertRaisesRegex(ValueError, "spot region row count"):
            validate_catalog_payloads(
                raw_instances=3,
                series_models=series_models,
                instance_metadata=instance_metadata,
                instance_regions=instance_regions,
                config=self.config,
            )

    def test_validate_dataset_dir_rejects_large_baseline_drop(self) -> None:
        series_models, instance_metadata, instance_regions = build_sample_payloads()
        with tempfile.TemporaryDirectory() as baseline_dir_name, tempfile.TemporaryDirectory() as candidate_dir_name:
            baseline_dir = Path(baseline_dir_name)
            candidate_dir = Path(candidate_dir_name)
            write_payload(baseline_dir / SERIES_MODELS_FILENAME, series_models)
            write_payload(baseline_dir / INSTANCE_METADATA_FILENAME, instance_metadata)
            write_payload(baseline_dir / INSTANCE_REGIONS_FILENAME, instance_regions)
            write_payload(
                candidate_dir / SERIES_MODELS_FILENAME,
                [
                    {
                        "series": "n2",
                        "instance_count": 2,
                        "instance_types": ["n2-standard-2", "n2-standard-4"],
                    }
                ],
            )
            write_payload(candidate_dir / INSTANCE_METADATA_FILENAME, instance_metadata[:2])
            write_payload(candidate_dir / INSTANCE_REGIONS_FILENAME, instance_regions[:2])

            with self.assertRaisesRegex(ValueError, "matched instance count"):
                validate_dataset_dir(
                    candidate_dir,
                    raw_instances=3,
                    baseline_dir=baseline_dir,
                    config=CatalogValidationConfig(
                        min_raw_instances=3,
                        min_matched_instances=2,
                        min_match_ratio=0.5,
                        min_series_count=1,
                        min_region_rows=2,
                        min_spot_region_rows=2,
                        max_missing_region_name_ratio=0.1,
                        min_metadata_baseline_ratio=1.0,
                        min_series_baseline_ratio=0.8,
                        min_regions_baseline_ratio=0.8,
                    ),
                )

    def test_normalize_gpu_model_aligns_with_aws_style(self) -> None:
        self.assertEqual(normalize_gpu_model("nvidia-a100-80gb"), "NVIDIA A100")
        self.assertEqual(normalize_gpu_model("nvidia-tesla-a100"), "NVIDIA A100")
        self.assertEqual(normalize_gpu_model("nvidia-h100-mega-80gb"), "NVIDIA H100")
        self.assertEqual(normalize_gpu_model("nvidia-h200-141gb"), "NVIDIA H200")
        self.assertEqual(normalize_gpu_model("nvidia-l4"), "NVIDIA L4")

    def test_normalize_architectures_aligns_with_aws_style(self) -> None:
        self.assertEqual(normalize_architectures(["amd64"]), ["x86_64"])
        self.assertEqual(normalize_architectures(["x86_64"]), ["x86_64"])
        self.assertEqual(normalize_architectures(["aarch64"]), ["arm64"])
        self.assertEqual(normalize_architectures(["arm64"]), ["arm64"])


def write_payload(path: Path, payload: list[dict]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
