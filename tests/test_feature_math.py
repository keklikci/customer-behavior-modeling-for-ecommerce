from datetime import datetime, timedelta

import pytest

from feature_math import (
    time_difference_list,
    weighted_mean,
    weighted_standard_deviation,
)


def test_time_difference_list_returns_seconds():
    start = datetime(2024, 1, 1, 12)
    values = [start, start + timedelta(seconds=30), start + timedelta(minutes=2)]

    assert time_difference_list(values) == [30, 90]


def test_time_difference_list_uses_zero_for_short_input():
    assert time_difference_list([]) == [0]
    assert time_difference_list([datetime(2024, 1, 1)]) == [0]


def test_weighted_mean_favors_recent_values():
    assert weighted_mean([0, 10]) == pytest.approx(8)


def test_weighted_standard_deviation_matches_weighted_values():
    mean = weighted_mean([0, 10])

    assert weighted_standard_deviation([0, 10], mean) == pytest.approx(4)
