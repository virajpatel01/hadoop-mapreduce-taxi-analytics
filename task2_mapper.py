#!/usr/bin/env python3

import math
import sys


def load_medoids(filename):
    # file format: cluster_id  medoid_x  medoid_y
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

    # sort by id so ordering doesn't depend on file order
    medoids.sort(key=lambda item: item[0])

    return medoids


def nearest_medoid(x, y, medoids):
    # euclidean distance to each medoid, return the closest cluster id
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
    # input_type "trips" = first iteration, reading Trips.txt directly
    # input_type "assignments" = later iterations, reading last round's output
    medoids = load_medoids(medoid_file)

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        if input_type == "trips":
            # trip_id,taxi_id,fare,distance,pickup_x,pickup_y,dropoff_x,dropoff_y
            fields = line.split(",")

            if len(fields) != 8:
                continue

            trip_id = fields[0]
            x_text = fields[6]
            y_text = fields[7]

            old_cluster = None

        elif input_type == "assignments":
            # cluster_id, trip_id, x, y, changed
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

        # no previous assignment on iteration 1, so treat it as changed
        if old_cluster is None:
            changed = 1
        elif old_cluster == new_cluster:
            changed = 0
        else:
            changed = 1

        # key = cluster_id, value = trip_id, x, y, changed
        print(
            f"{new_cluster}\t"
            f"{trip_id}\t"
            f"{x_text}\t"
            f"{y_text}\t"
            f"{changed}"
        )


def update_mapper():
    # combines repeated (cluster, x, y) coords within this mapper
    # before shuffling, to cut down traffic to the reducer
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
            # [point count, changed count]
            aggregates[key] = [1, changed]
        else:
            aggregates[key][0] += 1
            aggregates[key][1] += changed

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
    # strips changed_assignments off before the final output job
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