#!/usr/bin/env python3

import sys


def classify_trip(distance):
    """
    Classify a trip based on its distance.

    short  : distance < 100
    medium : 100 <= distance < 200
    long   : distance >= 200
    """
    if distance >= 200:
        return "long"
    elif distance >= 100:
        return "medium"
    else:
        return "short"


def main():
    # In-mapper combining state.
    #
    # Key:
    #   (taxi_id, trip_type)
    #
    # Value:
    #   [trip_count, fare_sum, min_fare, max_fare]
    #
    # This dictionary remains in memory across all input lines
    # processed by this mapper task.
    aggregates = {}

    for line in sys.stdin:
        line = line.strip()

        # Ignore empty lines.
        if not line:
            continue

        fields = line.split(",")

        # Trips.txt contains exactly eight fields:
        # trip_id,taxi_id,fare,distance,
        # pickup_x,pickup_y,dropoff_x,dropoff_y
        if len(fields) != 8:
            continue

        try:
            taxi_id = fields[1]
            fare = float(fields[2])
            distance = float(fields[3])
        except ValueError:
            # Ignore malformed numeric records instead of
            # terminating the entire Hadoop mapper.
            continue

        trip_type = classify_trip(distance)
        key = (taxi_id, trip_type)

        if key not in aggregates:
            # First trip seen for this taxi/trip-type combination.
            aggregates[key] = [1, fare, fare, fare]

        else:
            stats = aggregates[key]

            # Update trip count.
            stats[0] += 1

            # Update total fare.
            stats[1] += fare

            # Update minimum fare.
            if fare < stats[2]:
                stats[2] = fare

            # Update maximum fare.
            if fare > stats[3]:
                stats[3] = fare

    # Emit one partially aggregated record for every
    # taxi/trip-type combination seen by this mapper.
    for (taxi_id, trip_type), stats in aggregates.items():
        trip_count, fare_sum, min_fare, max_fare = stats

        # Keep taxi_id and trip_type together as the Hadoop key.
        # Everything before the first tab becomes the key.
        composite_key = f"{taxi_id},{trip_type}"

        print(
            f"{composite_key}\t"
            f"{trip_count}\t"
            f"{fare_sum}\t"
            f"{min_fare}\t"
            f"{max_fare}"
        )


if __name__ == "__main__":
    main()