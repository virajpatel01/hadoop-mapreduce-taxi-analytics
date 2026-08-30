#!/usr/bin/env python3

# Task 3, Job 1 reducer

import sys


def dump(taxi_id, company_id, trips):
    # Output trips once the company is known
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

    # Finish the previous taxi
    if current_taxi is not None and taxi_id != current_taxi:
        dump(current_taxi, company_id, trips)
        company_id = None
        trips = []

    current_taxi = taxi_id

    if tag == "T":
        # Company information
        company_id = parts[2]

    elif tag == "R":
        # Trip information
        fare = parts[2]
        distance = parts[3]
        trips.append((fare, distance))

# Output the last taxi
if current_taxi is not None:
    dump(current_taxi, company_id, trips)