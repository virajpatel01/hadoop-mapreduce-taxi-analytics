#!/bin/bash

# Task 3 - Company Performance Analysis
# Runs three chained MapReduce jobs on EMR:
#   Job 1: join Trips.txt with Taxis.txt
#   Job 2: aggregate per-company metrics
#   Job 3: sort companies by revenue, highest first
# Final result lands in /Output/task3.

set -euo pipefail


# ----- paths -----

TRIPS="/Input/Trips.txt"
TAXIS="/Input/Taxis.txt"
OUTPUT="/Output/task3"

# intermediate work goes here, kept out of /Output and cleaned up at the end
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


# ----- checks -----

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


# ----- clean old output and work dirs -----

hadoop fs -mkdir -p /Output

if hadoop fs -test -e "$OUTPUT"; then
    echo "Removing old output: $OUTPUT"
    hadoop fs -rm -r -f "$OUTPUT"
fi

if hadoop fs -test -e "$WORK"; then
    hadoop fs -rm -r -f "$WORK"
fi

hadoop fs -mkdir -p "$WORK"


echo "=========================================="
echo "Task 3 - Company Performance Analysis"
echo "=========================================="


# ----- JOB 1: JOIN -----
# Both files go in together. Mapper tags each record and keys on taxi id.
# Reducer attaches the company to every trip.

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


# ----- JOB 2: AGGREGATION -----
# Re-key on company, then total revenue/trips/distance and count
# distinct taxis (fleet size).

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


# ----- JOB 3: SORT -----
# Mapper adds a revenue band as the first key field.
# KeyFieldBasedPartitioner sends each band to its own reducer.
# KeyFieldBasedComparator sorts by band, then revenue descending.
# Merging reducer 0 + 1 + 2 gives a global high-to-low order.

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
    -mapper "python3 $J3_MAP" \
    -reducer "python3 $J3_RED" \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -input "$JOB2_OUT" \
    -output "$OUTPUT"


# ----- clean up intermediates -----

hadoop fs -rm -r -f "$WORK"


echo ""
echo "Task 3 finished."
echo "Final output: $OUTPUT"
echo ""
hadoop fs -ls "$OUTPUT"