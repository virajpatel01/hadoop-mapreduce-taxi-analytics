#!/bin/bash

# Task 3 - Company Performance Analysis

set -euo pipefail


# Paths

TRIPS="/Input/Trips.txt"
TAXIS="/Input/Taxis.txt"
OUTPUT="/Output/task3"

WORK="/tmp/task3_work"
JOB1_OUT="$WORK/job1"
JOB2_OUT="$WORK/job2"

STREAMING_JAR="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

J1_MAP="task3_job1_mapper.py"
J1_RED="task3_job1_reducer.py"
J2_MAP="task3_job2_mapper.py"
J2_RED="task3_job2_reducer.py"
J3_MAP="task3_job3_mapper.py"
J3_RED="task3_job3_reducer.py"


# Check required files

for f in "$J1_MAP" "$J1_RED" "$J2_MAP" "$J2_RED" "$J3_MAP" "$J3_RED"
do
    if [ ! -f "$f" ]; then
        echo "ERROR: missing file '$f'"
        exit 1
    fi
done

if [ ! -f "$STREAMING_JAR" ]; then
    echo "ERROR: streaming jar not found at $STREAMING_JAR"
    exit 1
fi

if ! hadoop fs -test -e "$TRIPS"; then
    echo "ERROR: $TRIPS not found in HDFS"
    exit 1
fi

if ! hadoop fs -test -e "$TAXIS"; then
    echo "ERROR: $TAXIS not found in HDFS"
    exit 1
fi


# Remove old output and temporary files

hadoop fs -mkdir -p /Output

if hadoop fs -test -e "$OUTPUT"; then
    echo "Removing old output: $OUTPUT"
    hadoop fs -rm -r -f "$OUTPUT"
fi

if hadoop fs -test -e "$WORK"; then
    hadoop fs -rm -r -f "$WORK"
fi

hadoop fs -mkdir -p "$WORK"


echo "Task 3 - Company Performance Analysis"


# Job 1: Join

echo ""
echo "--- Job 1: Join ---"

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="Task3_Job1_Join" \
    -D mapreduce.job.reduces=3 \
    -files "$J1_MAP","$J1_RED" \
    -mapper "python3 $J1_MAP" \
    -reducer "python3 $J1_RED" \
    -input "$TRIPS" \
    -input "$TAXIS" \
    -output "$JOB1_OUT"


# Job 2: Aggregation

echo ""
echo "--- Job 2: Aggregation ---"

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="Task3_Job2_Aggregate" \
    -D mapreduce.job.reduces=3 \
    -files "$J2_MAP","$J2_RED" \
    -mapper "python3 $J2_MAP" \
    -reducer "python3 $J2_RED" \
    -input "$JOB1_OUT" \
    -output "$JOB2_OUT"


# Job 3: Sort

echo ""
echo "--- Computing dynamic revenue cutoffs ---"

JOB2_LOCAL="task3_job2_output.tmp"

hadoop fs -cat "$JOB2_OUT"/part-* > "$JOB2_LOCAL"

CUTOFFS=$(python3 task3_compute_cutoffs.py "$JOB2_LOCAL")
HIGH_CUTOFF=$(echo "$CUTOFFS" | sed -n '1p')
LOW_CUTOFF=$(echo "$CUTOFFS" | sed -n '2p')

echo "High cutoff: $HIGH_CUTOFF"
echo "Low cutoff:  $LOW_CUTOFF"

rm -f "$JOB2_LOCAL"

echo ""
echo "--- Job 3: Sort by revenue (descending) ---"

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="Task3_Job3_Sort" \
    -D mapreduce.job.reduces=3 \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.map.output.key.field.separator=$'\t' \
    -D mapreduce.partition.keypartitioner.options=-k1,1 \
    -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2n" \
    -files "$J3_MAP","$J3_RED" \
    -mapper "python3 $J3_MAP $HIGH_CUTOFF $LOW_CUTOFF" \
    -reducer "python3 $J3_RED" \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -input "$JOB2_OUT" \
    -output "$OUTPUT"


# Clean up temporary files

hadoop fs -rm -r -f "$WORK"


echo ""
echo "Task 3 finished."
echo "Final output: $OUTPUT"
echo ""
hadoop fs -ls "$OUTPUT"