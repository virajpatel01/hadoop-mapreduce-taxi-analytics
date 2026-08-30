#!/usr/bin/env python3

# Task 3 helper script

import sys


def main():
    if len(sys.argv) != 2:
        print("Usage: task3_compute_cutoffs.py <job2_output_file>", file=sys.stderr)
        sys.exit(1)

    revenues = []

    with open(sys.argv[1], "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) != 6:
                continue

            try:
                revenue = float(parts[1])
            except ValueError:
                continue

            revenues.append(revenue)

    if not revenues:
        print("ERROR: no company revenue records found.", file=sys.stderr)
        sys.exit(1)

    # Sort revenues from lowest to highest
    revenues.sort()

    n = len(revenues)

    # Find the one-third and two-thirds positions
    low_index = n // 3
    high_index = (2 * n) // 3

    # Keep the indexes within the list
    low_index = min(low_index, n - 1)
    high_index = min(high_index, n - 1)

    low_cutoff = revenues[low_index]
    high_cutoff = revenues[high_index]

    print(f"{high_cutoff:.2f}")
    print(f"{low_cutoff:.2f}")


if __name__ == "__main__":
    main()