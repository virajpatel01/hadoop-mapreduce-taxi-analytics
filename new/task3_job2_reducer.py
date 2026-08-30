#!/usr/bin/env python3

# Task 3, Job 2 reducer

import sys


def dump(company_id, revenue, trips, distance_sum, taxis):
    if trips == 0:
        return

    fleet_size = len(taxis)

    if fleet_size > 0:
        revenue_per_taxi = revenue / fleet_size
    else:
        revenue_per_taxi = 0.0

    avg_distance = distance_sum / trips

    print(
        company_id + "\t"
        + format(revenue, ".2f") + "\t"
        + str(trips) + "\t"
        + str(fleet_size) + "\t"
        + format(revenue_per_taxi, ".2f") + "\t"
        + format(avg_distance, ".2f")
    )


current_company = None
revenue = 0.0
trips = 0
distance_sum = 0.0
taxis = set()

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    company_id = parts[0]
    taxi_id = parts[1]

    try:
        fare = float(parts[2])
        distance = float(parts[3])
    except ValueError:
        continue

    # Finish the previous company
    if current_company is not None and company_id != current_company:
        dump(current_company, revenue, trips, distance_sum, taxis)
        revenue = 0.0
        trips = 0
        distance_sum = 0.0
        taxis = set()

    current_company = company_id

    revenue += fare
    trips += 1
    distance_sum += distance
    taxis.add(taxi_id)

# Output the last company
if current_company is not None:
    dump(current_company, revenue, trips, distance_sum, taxis)