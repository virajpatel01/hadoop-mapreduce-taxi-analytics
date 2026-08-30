#!/bin/bash

# Task 1 - Taxi-Level Trip Efficiency Analysis

set -euo pipefail

INPUT="/Input/Trips.txt"
OUTPUT="/Output/task1"

MAPPER="task1_mapper.py"
REDUCER="task1_reducer.py"

STREAMING_JAR="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

echo "Starting Task 1"

# check the local files we need are actually here
if [ ! -f "$MAPPER" ]; then
    echo "ERROR: $MAPPER not found."
    exit 1
fi

if [ ! -f "$REDUCER" ]; then
    echo "ERROR: $REDUCER not found."
    exit 1
fi

if [ ! -f "$STREAMING_JAR" ]; then
    echo "ERROR: Hadoop Streaming JAR not found at:"
    echo "$STREAMING_JAR"
    exit 1
fi

if ! hadoop fs -test -e "$INPUT"; then
    echo "ERROR: HDFS input file $INPUT does not exist."
    exit 1
fi

hadoop fs -mkdir -p /Output

#