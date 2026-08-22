#!/usr/bin/env python3

import math
import sys


def load_medoids(filename):
    """
    Load the current medoids from a local file distributed
    to the mapper through Hadoop Streaming.

    Expected format:
        cluster_id    medoid_x    medoid_y
    """
    medoids = []

    with open(filename, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if not line:
                continue

            fields = line.split()

            if len(fields) < 3:
                continue

            try:
                cluster_id = int(fields[0])
                medoid_x = float(fields[1])
                medoid_y = float(fields[2])
            except ValueError:
                continue

            medoids.append((cluster_id, medoid_x, medoid_y))

    if not medoids:
        raise RuntimeError("No valid medoids were loaded.")

    # Keep cluster ordering deterministic.
    medoids.sort(key=lambda item: item[0])

    return medoids


def nearest_medoid(x, y, medoids):
    """
    Return the ID of the medoid closest to point (x, y)
    using Euclidean distance.
    """
    best_cluster = None
    best_distance = None

    for cluster_id, medoid_x, medoid_y in medoids:
        distance = math.hypot(
            x - medoid_x,
            y - medoid_y
        )

        if best_distance is None or distance < best_distance:
            best_cluster = cluster_id
            best_distance = distance

    return best_cluster


def assignment_mapper(medoid_file, input_type):
    """
    Assign every drop-off point to its nearest current medoid.

    input_type = "trips"
        Input is the original Trips.txt file.

    input_type = "assignments"
        Input is the previous iteration's assignment output.
    """
    medoids = load_medoids(medoid_file)

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        if input_type == "trips":
            # Trips.txt:
            # trip_id,taxi_id,fare,distance,
            # pickup_x,pickup_y,dropoff_x,dropoff_y
            fields = line.split(",")

            if len(fields) != 8:
                continue

            trip_id = fields[0]
            x_text = fields[6]
            y_text = fields[7]

            old_cluster = None

        elif input_type == "assignments":
            # Previous assignment output:
            # cluster_id <tab> trip_id <tab>
            # x <tab> y <tab> changed
            fields = line.split("\t")

            if len(fields) != 5:
                continue

            try:
                old_cluster = int(fields[0])
            except ValueError:
                continue

            trip_id = fields[1]
            x_text = fields[2]
            y_text = fields[3]

        else:
            raise ValueError(
                "input_type must be 'trips' or 'assignments'"
            )

        try:
            x = float(x_text)
            y = float(y_text)
        except ValueError:
            continue

        new_cluster = nearest_medoid(x, y, medoids)

        # The first iteration has no previous assignment,
        # so every point is considered newly assigned.
        if old_cluster is None:
            changed = 1
        elif old_cluster == new_cluster:
            changed = 0
        else:
            changed = 1

        # Hadoop key:
        #   cluster_id
        #
        # Hadoop value:
        #   trip_id, x, y, changed
        print(
            f"{new_cluster}\t"
            f"{trip_id}\t"
            f"{x_text}\t"
            f"{y_text}\t"
            f"{changed}"
        )


def update_mapper():
    """
    Partially aggregate assigned drop-off locations before the
    PAM update reducer.

    Input:
        cluster_id <tab> trip_id <tab> x <tab> y <tab> changed

    Output:
        cluster_id <tab> x <tab> y <tab>
        point_count <tab> changed_count

    Repeated coordinates are combined within each mapper task
    to reduce shuffle traffic.
    """
    aggregates = {}

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        fields = line.split("\t")

        if len(fields) != 5:
            continue

        cluster_id = fields[0]
        x_text = fields[2]
        y_text = fields[3]
        changed_text = fields[4]

        try:
            int(cluster_id)
            float(x_text)
            float(y_text)
            changed = int(changed_text)
        except ValueError:
            continue

        key = (cluster_id, x_text, y_text)

        if key not in aggregates:
            # [number of points at this coordinate,
            #  number whose cluster assignment changed]
            aggregates[key] = [1, changed]
        else:
            aggregates[key][0] += 1
            aggregates[key][1] += changed

    # Emit one partial frequency record per
    # cluster/coordinate combination.
    for (cluster_id, x_text, y_text), values in aggregates.items():
        point_count, changed_count = values

        print(
            f"{cluster_id}\t"
            f"{x_text}\t"
            f"{y_text}\t"
            f"{point_count}\t"
            f"{changed_count}"
        )


def final_mapper():
    """
    Prepare the final PAM update result for the final reducer.

    Input:
        cluster_id <tab>
        medoid_x <tab>
        medoid_y <tab>
        number_of_points <tab>
        average_dissimilarity <tab>
        changed_assignments

    Output:
        cluster_id <tab>
        medoid_x <tab>
        medoid_y <tab>
        number_of_points <tab>
        average_dissimilarity
    """
    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        fields = line.split("\t")

        if len(fields) != 6:
            continue

        cluster_id = fields[0]
        medoid_x = fields[1]
        medoid_y = fields[2]
        point_count = fields[3]
        average_dissimilarity = fields[4]

        try:
            int(cluster_id)
            float(medoid_x)
            float(medoid_y)
            int(point_count)
            float(average_dissimilarity)
        except ValueError:
            continue

        # Keep cluster_id as the Hadoop key during shuffle.
        print(
            f"{cluster_id}\t"
            f"{medoid_x}\t"
            f"{medoid_y}\t"
            f"{point_count}\t"
            f"{average_dissimilarity}"
        )


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: task2_mapper.py "
            "<assign|update|final> [arguments]",
            file=sys.stderr
        )
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "assign":

        if len(sys.argv) != 4:
            print(
                "Usage: task2_mapper.py assign "
                "<medoid_file> <trips|assignments>",
                file=sys.stderr
            )
            sys.exit(1)

        medoid_file = sys.argv[2]
        input_type = sys.argv[3]

        assignment_mapper(
            medoid_file,
            input_type
        )

    elif mode == "update":
        update_mapper()

    elif mode == "final":
        final_mapper()

    else:
        print(
            f"Unknown mapper mode: {mode}",
            file=sys.stderr
        )
        sys.exit(1)


if __name__ == "__main__":
    main()