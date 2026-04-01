from __future__ import annotations

from app.etl.orchestration.cotahist_adaptive_strategy import (
    AdaptiveSizeThresholdsMiB,
    classify_size_bucket,
)

MIB = 1024 * 1024


def test_classify_size_bucket_standard_on_exact_200_mib() -> None:
    assert classify_size_bucket(200 * MIB) == "standard"


def test_classify_size_bucket_medium_just_above_200_mib() -> None:
    assert classify_size_bucket(200 * MIB + 1) == "medium"


def test_classify_size_bucket_medium_on_exact_700_mib() -> None:
    assert classify_size_bucket(700 * MIB) == "medium"


def test_classify_size_bucket_large_just_above_700_mib() -> None:
    assert classify_size_bucket(700 * MIB + 1) == "large"


def test_classify_size_bucket_nominal_representatives() -> None:
    assert classify_size_bucket(40 * MIB) == "standard"
    assert classify_size_bucket(420 * MIB) == "medium"
    assert classify_size_bucket(1_024 * MIB) == "large"


def test_classification_depends_on_size_not_filename() -> None:
    size = 256 * MIB
    assert classify_size_bucket(size, source_file_name="COTAHIST_A2001.TXT") == "medium"
    assert classify_size_bucket(size, source_file_name="totally_different_name.bin") == "medium"


def test_thresholds_are_configurable_with_correct_defaults() -> None:
    defaults = AdaptiveSizeThresholdsMiB()
    assert defaults.standard_max_mib == 200
    assert defaults.medium_max_mib == 700

    custom = AdaptiveSizeThresholdsMiB(standard_max_mib=10, medium_max_mib=20)
    assert classify_size_bucket(10 * MIB, thresholds=custom) == "standard"
    assert classify_size_bucket(10 * MIB + 1, thresholds=custom) == "medium"
    assert classify_size_bucket(20 * MIB + 1, thresholds=custom) == "large"
