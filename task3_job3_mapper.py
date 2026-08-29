#!/usr/bin/env python3

# Task 3, Job 3 mapper (sorting).
# Input is Job 2's output:
#   company, revenue, trips, fleet, rev_per_taxi, avg_dist
#
# Goal: final result sorted by revenue, HIGHEST first, split across
# 3 reducers so reducer 0 = top band, reducer 1 = middle, reducer 2 = bottom.
#
# Trick (order inversion, taught in the course): instead of relying on a
# reverse comparator, I build a numeric sort key that already sorts the
# right way under a plain ascending sort. I use (BIG - revenue) as the
# sort key, so the biggest revenue gives the smallest sort key and lands
# first. No "-nr" needed anywhere.
#
# I also add a band number as the FIRST key field so the partitioner can
# send each band to its own reducer.
#
# Output:
#   band <tab> sortkey <tab> company <tab> revenue <tab> trips <tab> fleet <tab> rev_per_taxi <tab> avg_dist
# The first TWO fields (band, sortkey) are the key. The rest is the value.

import sys

# A number safely bigger than any possible company revenue.
BIG = 1000000000.0

# Revenue cut-offs that decide the band. Tune these to the real data so the
# three reducers each get a share. band 0 = top, band 1 = middle, band 2 = bottom.
HIGH_CUTOFF = 285000.0   # revenue >= this  -> band 0 (top)
LOW_CUTOFF = 250000.0    # revenue >= this  -> band 1 (middle), else band 2 (bottom)


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

    # sort key: big minus revenue, so highest revenue -> smallest key -> comes first
    sortkey = BIG - revenue

    # band and sortkey are the key (first 2 fields); the rest is the value.
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