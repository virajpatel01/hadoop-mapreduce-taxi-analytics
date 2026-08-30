#!/usr/bin/env python3

# Task 3, Job 3 reducer

import sys


for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")

    if len(parts) != 8:
        continue

    company = parts[2]
    revenue = parts[3]
    trips = parts[4]
    fleet = parts[5]
    rev_per_taxi = parts[6]
    avg_dist = parts[7]

    print(
        company + "\t"
        + revenue + "\t"
        + trips + "\t"
        + fleet + "\t"
        + rev_per_taxi + "\t"
        + avg_dist
    )