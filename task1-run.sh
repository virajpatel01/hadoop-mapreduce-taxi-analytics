#!/bin/bash

# Task 1 - Taxi-Level Trip Efficiency Analysis
# Runs the complete Hadoop Streaming job on the RMIT EMR cluster.

set -euo pipefail

INPUT="/Input/Trips.txt"
OUTPUT="/Output/task1"

MAPPER="task1_mapper.py"
REDUCER="task1_reducer.py"

STREAMING_JAR="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

echo "=========================================="
echo "Task 1 - Taxi-Level Trip Efficiency"
echo "=========================================="

# Check required local program files.
if [ ! -f "$MAPPER" ]; then
    echo "ERROR: $MAPPER not found."
    exit 1
fi

if [ ! -f "$REDUCER" ]; then
    echo "ERROR: $REDUCER not found."
    exit 1
fi

# Check Hadoop Streaming JAR.
if [ ! -f "$STREAMING_JAR" ]; then
    echo "ERROR: Hadoop Streaming JAR not found at:"
    echo "$STREAMING_JAR"
    exit 1
fi

# Check that the required HDFS input exists.
if ! hadoop fs -test -e "$INPUT"; then
    echo "ERROR: HDFS input file $INPUT does not exist."
    exit 1
fi

# Ensure the required parent output directory exists.
hadoop fs -mkdir -p /Output

# Hadoop cannot write to an existing output directory.
# Remove only the previous Task 1 output if it exists.
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

# Display generated Hadoop output files.
hadoop fs -ls "$OUTPUT"