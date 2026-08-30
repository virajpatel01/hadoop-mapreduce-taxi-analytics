#!/usr/bin/env python3

from dataclasses import fields
import sys
from decimal import Decimal, InvalidOperation


def emit_result(key, total_count, total_fare, min_fare, max_fare):
    """
    Output the final statistics for one taxi/trip-type combination.

    Final format:
    taxi_id <tab> trip_type <tab> trip_count <tab>
    max_fare <tab> min_fare <tab> average_fare
    """
    taxi_id, trip_type = key.split(",", 1)

    average_fare = total_fare / total_count

    print(
        f"{taxi_id}\t"
        f"{trip_type}\t"
        f"{total_count}\t"
        f"{max_fare:.2f}\t"
        f"{min_fare:.2f}\t"
        f"{average_fare:.2f}"
    )


def main():
    current_key = None

    total_count = 0
    total_fare = Decimal("0")
    min_fare = None
    max_fare = None

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        fields = line.split("\t")

        # Mapper output contains:
        # composite_key, partial_count, partial_sum,
        # partial_min, partial_max
        if len(fields) != 5:
            continue

        key = fields[0]

        try:
            partial_count = int(fields[1])
            partial_sum = Decimal(fields[2])
            partial_min = Decimal(fields[3])
            partial_max = Decimal(fields[4])
        except (ValueError, InvalidOperation):
            # Skip malformed intermediate records.
            continue

        if current_key is None:
            # Initialise aggregation for the first key.
            current_key = key
            total_count = partial_count
            total_fare = partial_sum
            min_fare = partial_min
            max_fare = partial_max

        elif key == current_key:
            # Merge another mapper's partial statistics
            # for the same taxi/trip-type combination.
            total_count += partial_count
            total_fare += partial_sum

            if partial_min < min_fare:
                min_fare = partial_min

            if partial_max > max_fare:
                max_fare = partial_max

        else:
            # Hadoop provides sorted mapper output.
            # When the key changes, the previous group is complete.
            emit_result(
                current_key,
                total_count,
                total_fare,
                min_fare,
                max_fare
            )

            # Start aggregation for the new key.
            current_key = key
            total_count = partial_count
            total_fare = partial_sum
            min_fare = partial_min
            max_fare = partial_max

    # The final key has no later key change to trigger emission,
    # so output it explicitly after reaching EOF.
    if current_key is not None:
        emit_result(
            current_key,
            total_count,
            total_fare,
            min_fare,
            max_fare
        )


if __name__ == "__main__":
    main()