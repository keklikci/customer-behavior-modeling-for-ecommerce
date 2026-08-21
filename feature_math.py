"""Pure Python helpers used by the feature engineering pipeline."""

import math
from collections.abc import Sequence


def time_difference_list(values: Sequence) -> list[float]:
    """Return elapsed seconds between consecutive datetime values."""
    if len(values) < 2:
        return [0]
    return [
        (second - first).total_seconds()
        for first, second in zip(values, values[1:])
    ]


def weighted_mean(values: Sequence[float]) -> float:
    """Return a recency weighted mean with squared positional weights."""
    if not values:
        return 0
    weights = [(index + 1) ** 2 for index in range(len(values))]
    return sum(weight * value for weight, value in zip(weights, values)) / sum(
        weights
    )


def weighted_standard_deviation(values: Sequence[float], mean: float) -> float:
    """Return a recency weighted standard deviation."""
    if not values:
        return 0
    weights = [(index + 1) ** 2 for index in range(len(values))]
    variance = sum(
        weight * (value - mean) ** 2 for weight, value in zip(weights, values)
    ) / sum(weights)
    return math.sqrt(variance)

