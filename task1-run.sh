#!/bin/bash

# Task 1 - Taxi-Level Trip Efficiency Analysis

set -euo pipefail

INPUT="/Input/Trips.txt"
OUTPUT="/Output/task1"

MAPPER="task1_mapper.py"
REDUCER="task1_reducer.py"

STREAMING_JAR="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

echo "Starting Task 1"

# Check required files
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

# Remove old output
if hadoop fs -test -e "$OUTPUT"; then
    echo "Removing previous output: $OUTPUT"
    hadoop fs -rm -r -f "$OUTPUT"
fi

echo "Running Hadoop Streaming job with 3 reducers..."

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="Task1_Taxi_Trip_Efficiency" \
    -D mapreduce.job.reduces=3 \
    -files "$MAPPER","$REDUCER" \
    -mapper "python3 $MAPPER" \
    -reducer "python3 $REDUCER" \
    -input "$INPUT" \
    -output "$OUTPUT"

echo
echo "Task 1 completed successfully."
echo "Final HDFS output: $OUTPUT"
echo

hadoop fs -ls "$OUTPUT"
