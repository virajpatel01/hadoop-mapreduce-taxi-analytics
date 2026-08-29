#!/usr/bin/env python3

# Task 3, Job 1 reducer.
# Input comes in sorted by taxi id, so every line for one taxi
# shows up together. For each taxi I pull out its company from the
# T line, then stick that company onto every trip (the R lines).
#
# I keep the taxi id in the output on purpose. Job 2 needs it to
# work out fleet size (how many different taxis a company has).

import sys


def dump(taxi_id, company_id, trips):
    # print every trip for this taxi, now that we know the company.
    # if we never saw a company for it, we can't tag the trips, so skip.
    if company_id is None:
        return
    for fare, distance in trips:
        print(company_id + "\t" + taxi_id + "\t" + fare + "\t" + distance)


current_taxi = None
company_id = None
trips = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 2:
        continue

    taxi_id = parts[0]
    tag = parts[1]

    # new taxi id means the last one is finished, so flush it first
    if current_taxi is not None and taxi_id != current_taxi:
        dump(current_taxi, company_id, trips)
        company_id = None
        trips = []

    current_taxi = taxi_id

    if tag == "T":
        # company info line
        company_id = parts[2]
    elif tag == "R":
        # trip line, hang on to the fare and distance
        fare = parts[2]
        distance = parts[3]
        trips.append((fare, distance))

# don't forget the very last taxi once the input runs out
if current_taxi is not None:
    dump(current_taxi, company_id, trips)