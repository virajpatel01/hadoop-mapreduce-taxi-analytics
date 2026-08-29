#!/usr/bin/env python3

# Task 3, Job 2 mapper.
# Input is Job 1's output:  company_id, taxi_id, fare, distance
# I just re-key it on the company id so Hadoop groups every trip
# by company. I pass the taxi id along too because the reducer
# needs it to count distinct taxis (fleet size).

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

    # key = company_id, value = taxi_id, fare, distance
    print(company_id + "\t" + taxi_id + "\t" + fare + "\t" + distance)