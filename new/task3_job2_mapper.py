#!/usr/bin/env python3

# Task 3, Job 2 mapper

import sys


for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    company_id = parts[0]
    taxi_id = parts[1]
    fare = parts[2]
    distance = parts[3]

    # Group records by company
    print(company_id + "\t" + taxi_id + "\t" + fare + "\t" + distance)