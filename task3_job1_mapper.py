#!/usr/bin/env python3

# Task 3, Job 1: the join.
# Both Taxis.txt and Trips.txt get fed into this mapper together.
# I need to tell them apart, then send both out keyed on the taxi id
# so Hadoop drops each taxi's company line next to all its trips.
#
# Easy way to tell the files apart: count the fields.
#   taxi line  -> 4 fields (taxi_id, company_id, model, year)
#   trip line  -> 8 fields (trip_id, taxi_id, fare, distance, + coords)

import sys


for line in sys.stdin:
    line = line.strip()

    # skip blank lines
    if not line:
        continue

    parts = line.split(",")

    # 4 fields means it is a taxi record
    if len(parts) == 4:
        taxi_id = parts[0]
        company_id = parts[1]

        # tag it "T" so the reducer knows this is the company info.
        # key = taxi_id, value = T and the company
        print(taxi_id + "\tT\t" + company_id)

    # 8 fields means it's a trip record
    elif len(parts) == 8:
        taxi_id = parts[1]
        fare = parts[2]
        distance = parts[3]

        # tag it "R" for a real trip row.
        # key = taxi_id, value = R, fare, distance
        print(taxi_id + "\tR\t" + fare + "\t" + distance)

    # anything else is junk, ignore it
    else:
        continue