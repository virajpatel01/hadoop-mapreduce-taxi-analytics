#!/usr/bin/env python3

# Task 3, Job 3 reducer (sorting).
# Rows arrive already sorted (by band, then sortkey ascending, which means
# revenue descending) and already split into the right reducer by the
# partitioner. Nothing to compute. I just drop the band and sortkey and
# print the six required fields:
#
#   company, revenue, trips, fleet, rev_per_taxi, avg_dist

import sys


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")

    # incoming: band, sortkey, company, revenue, trips, fleet, rev_per_taxi, avg_dist
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