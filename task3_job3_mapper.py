#!/usr/bin/env python3

# Task 3, Job 3 mapper

import sys


# Value used to reverse the revenue sort
BIG = 1000000000.0

if len(sys.argv) != 3:
    print("Usage: task3_job3_mapper.py <high_cutoff> <low_cutoff>", file=sys.stderr)
    sys.exit(1)

HIGH_CUTOFF = float(sys.argv[1])
LOW_CUTOFF = float(sys.argv[2])


def band_for(revenue):
    if revenue >= HIGH_CUTOFF:
        return 0
    elif revenue >= LOW_CUTOFF:
        return 1
    else:
        return 2


for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 6:
        continue

    company = parts[0]

    try:
        revenue = float(parts[1])
    except ValueError:
        continue

    trips = parts[2]
    fleet = parts[3]
    rev_per_taxi = parts[4]
    avg_dist = parts[5]

    band = band_for(revenue)

    # Higher revenue gets a smaller sort key
    sortkey = BIG - revenue

    print(
        str(band) + "\t"
        + format(sortkey, ".2f") + "\t"
        + company + "\t"
        + parts[1] + "\t"
        + trips + "\t"
        + fleet + "\t"
        + rev_per_taxi + "\t"
        + avg_dist
    )