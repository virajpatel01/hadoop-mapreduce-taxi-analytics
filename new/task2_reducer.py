#!/usr/bin/env python3

import math
import sys


def format_coordinate(value):
    # drop trailing zeros but keep full precision
    return format(value, ".15g")


def evaluate_cluster(cluster_id, coordinates):
    # coordinates: {(x, y): [point_count, changed_count]}
    # frequencies act as weights so this is the same as checking every trip one by one
    total_points = 0
    total_changed = 0

    for point_count, changed_count in coordinates.values():
        total_points += point_count
        total_changed += changed_count

    if total_points == 0:
        return

    best_coordinate = None
    best_average_dissimilarity = None

    # PAM swap step: try every assigned point as the new medoid
    for candidate_x, candidate_y in coordinates.keys():

        total_dissimilarity = 0.0

        for (point_x, point_y), values in coordinates.items():
            point_count = values[0]

            distance = math.hypot(
                candidate_x - point_x,
                candidate_y - point_y
            )

            total_dissimilarity += (
                distance * point_count
            )

        average_dissimilarity = (
            total_dissimilarity / total_points
        )

        candidate_coordinate = (
            candidate_x,
            candidate_y
        )

        # keep whichever candidate has the lowest cost
        # ties broken by coordinate order so results stay consistent
        if best_average_dissimilarity is None:
            best_coordinate = candidate_coordinate
            best_average_dissimilarity = (
                average_dissimilarity
            )

        elif (
            average_dissimilarity
            < best_average_dissimilarity - 1e-12
        ):
            best_coordinate = candidate_coordinate
            best_average_dissimilarity = (
                average_dissimilarity
            )

        elif (
            abs(
                average_dissimilarity
                - best_average_dissimilarity
            )
            <= 1e-12
            and candidate_coordinate < best_coordinate
        ):
            best_coordinate = candidate_coordinate
            best_average_dissimilarity = (
                average_dissimilarity
            )

    medoid_x, medoid_y = best_coordinate

    # cluster_id, medoid_x, medoid_y, points, avg_dissimilarity, changed
    print(
        f"{cluster_id}\t"
        f"{format_coordinate(medoid_x)}\t"
        f"{format_coordinate(medoid_y)}\t"
        f"{total_points}\t"
        f"{best_average_dissimilarity:.2f}\t"
        f"{total_changed}"
    )


def update_reducer():
    # hadoop groups by cluster_id, sorted, so we can stream through
    current_cluster = None

    # distinct coords + their counts for the cluster we're on
    coordinates = {}

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        fields = line.split("\t")

        if len(fields) != 5:
            continue

        cluster_id = fields[0]

        try:
            int(cluster_id)

            x = float(fields[1])
            y = float(fields[2])

            point_count = int(fields[3])
            changed_count = int(fields[4])

        except ValueError:
            continue

        if point_count <= 0:
            continue

        coordinate = (x, y)

        if current_cluster is None:
            current_cluster = cluster_id

        # cluster changed, previous one is done
        if cluster_id != current_cluster:

            evaluate_cluster(
                current_cluster,
                coordinates
            )

            current_cluster = cluster_id
            coordinates = {}

        # merge partials from different mappers for the same coordinate
        if coordinate not in coordinates:
            coordinates[coordinate] = [
                point_count,
                changed_count
            ]
        else:
            coordinates[coordinate][0] += point_count
            coordinates[coordinate][1] += changed_count

    # last cluster has no next key to trigger the emit above
    if current_cluster is not None:
        evaluate_cluster(
            current_cluster,
            coordinates
        )


def final_reducer():
    # drops cluster_id and changed_assignments, keeps just what task2 requires
    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        fields = line.split("\t")

        if len(fields) != 5:
            continue

        medoid_x = fields[1]
        medoid_y = fields[2]
        point_count = fields[3]
        average_dissimilarity = fields[4]

        try:
            float(medoid_x)
            float(medoid_y)
            int(point_count)
            float(average_dissimilarity)
        except ValueError:
            continue

        print(
            f"{medoid_x}\t"
            f"{medoid_y}\t"
            f"{point_count}\t"
            f"{float(average_dissimilarity):.2f}"
        )

def main():
    if len(sys.argv) != 2:
        print(
            "Usage: task2_reducer.py <update|final>",
            file=sys.stderr
        )
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "update":
        update_reducer()

    elif mode == "final":
        final_reducer()

    else:
        print(
            f"Unknown reducer mode: {mode}",
            file=sys.stderr
        )
        sys.exit(1)


if __name__ == "__main__":
    main()