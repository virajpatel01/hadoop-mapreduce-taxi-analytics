#!/usr/bin/env python3

# Task 3, Job 1: Join taxi and trip records

import sys


for line in sys.stdin:
    line = line.strip()

    # Skip blank lines
    if not line:
        continue

    parts = line.split(",")

    # Taxi record
    if len(parts) == 4:
        taxi_id = parts[0]
        company_id = parts[1]

        # T = taxi/company information
        print(taxi_id + "\tT\t" + company_id)

    # Trip record
    elif len(parts) == 8:
        taxi_id = parts[1]
        fare = parts[2]
        distance = parts[3]

        # R = trip information
        print(taxi_id + "\tR\t" + fare + "\t" + distance)

    # Ignore invalid records
    else:
        continue