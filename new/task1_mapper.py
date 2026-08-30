#!/usr/bin/env python3

import sys


def emit_result(key, total_count, total_fare, min_fare, max_fare):
    # taxi_id, trip_type, count, max_fare, min_fare, avg_fare
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
    total_fare = 0.0
    min_fare = None
    max_fare = None

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        fields = line.split("\t")

        # key, count, sum, min, max from the mapper
        if len(fields) != 5:
            continue

        key = fields[0]

        try:
            partial_count = int(fields[1])
            partial_sum = float(fields[2])
            partial_min = float(fields[3])
            partial_max = float(fields[4])
        except ValueError:
            continue

        if current_key is None:
            current_key = key
            total_count = partial_count
            total_fare = partial_sum
            min_fare = partial_min
            max_fare = partial_max

        elif key == current_key:
            # same key, merge partials from other mappers
            total_count += partial_count
            total_fare += partial_sum

            if partial_min < min_fare:
                min_fare = partial_min

            if partial_max > max_fare:
                max_fare = partial_max

        else:
            # key changed, so the previous group is done (input is sorted)
            emit_result(
                current_key,
                total_count,
                total_fare,
                min_fare,
                max_fare
            )

            current_key = key
            total_count = partial_count
            total_fare = partial_sum
            min_fare = partial_min
            max_fare = partial_max

    # last group has no next key to trigger the emit above
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