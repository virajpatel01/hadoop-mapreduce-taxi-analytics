#!/bin/bash

# Task 2 - Clustering Taxi Trips by Dropoff Location
# Iterative PAM (k-medoid) clustering using Hadoop Streaming.

set -euo pipefail

INPUT="/Input/Trips.txt"
OUTPUT="/Output/task2"
WORK_ROOT="/tmp/task2_work"

MAPPER="task2_mapper.py"
REDUCER="task2_reducer.py"
INITIALIZATION="initialization.txt"

STREAMING_JAR="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

CURRENT_MEDOIDS="task2_current_medoids.tmp"
ITERATION_RESULT="task2_iteration_result.tmp"

cleanup_local_files() {
    rm -f "$CURRENT_MEDOIDS" "$ITERATION_RESULT"
}

trap cleanup_local_files EXIT

for file in "$MAPPER" "$REDUCER" "$INITIALIZATION"
do
    if [ ! -f "$file" ]; then
        echo "ERROR: Required file '$file' was not found."
        exit 1
    fi
done

if [ ! -f "$STREAMING_JAR" ]; then
    echo "ERROR: Hadoop Streaming JAR was not found."
    exit 1
fi

if ! hadoop fs -test -e "$INPUT"; then
    echo "ERROR: HDFS input '$INPUT' does not exist."
    exit 1
fi

# first line of initialization.txt is v, the max iterations
MAX_ITERATIONS=$(
    sed -n '1p' "$INITIALIZATION" |
    tr -d '\r' |
    xargs
)

case "$MAX_ITERATIONS" in
    ''|*[!0-9]*)
        echo "ERROR: Invalid maximum iteration value."
        exit 1
        ;;
esac

if [ "$MAX_ITERATIONS" -lt 1 ]; then
    echo "ERROR: Maximum iterations must be at least 1."
    exit 1
fi

# rest of the lines are the initial medoids, turn them into
# cluster_id, medoid_x, medoid_y so k isn't hard-coded anywhere
awk '
    BEGIN {
        cluster_id = 0
    }

    NR > 1 && NF >= 2 {
        print cluster_id "\t" $1 "\t" $2
        cluster_id++
    }
' "$INITIALIZATION" > "$CURRENT_MEDOIDS"


K=$(awk 'END {print NR}' "$CURRENT_MEDOIDS")

if [ "$K" -lt 1 ]; then
    echo "ERROR: No initial medoids were found."
    exit 1
fi

echo "Task 2 - PAM k-Medoid Clustering"
echo "k = $K"
echo "Maximum iterations = $MAX_ITERATIONS"
echo

hadoop fs -mkdir -p /Output

if hadoop fs -test -e "$OUTPUT"; then
    echo "Removing previous output: $OUTPUT"
    hadoop fs -rm -r -f "$OUTPUT"
fi

if hadoop fs -test -e "$WORK_ROOT"; then
    hadoop fs -rm -r -f "$WORK_ROOT"
fi

hadoop fs -mkdir -p "$WORK_ROOT"

ITERATION=1
PREVIOUS_ASSIGNMENT=""
LAST_UPDATE=""

while [ "$ITERATION" -le "$MAX_ITERATIONS" ]
do
    ASSIGNMENT_OUTPUT="$WORK_ROOT/assignment_$ITERATION"
    UPDATE_OUTPUT="$WORK_ROOT/update_$ITERATION"

    echo "Iteration $ITERATION"

    # iteration 1 reads Trips.txt directly, later ones read the
    # previous round's assignment output so changes can be tracked
    if [ "$ITERATION" -eq 1 ]; then
        ASSIGNMENT_INPUT="$INPUT"
        INPUT_TYPE="trips"
    else
        ASSIGNMENT_INPUT="$PREVIOUS_ASSIGNMENT"
        INPUT_TYPE="assignments"
    fi

    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.name="Task2_Assignment_$ITERATION" \
        -D mapreduce.job.reduces=3 \
        -files "$MAPPER","$CURRENT_MEDOIDS" \
        -mapper "python3 $MAPPER assign $CURRENT_MEDOIDS $INPUT_TYPE" \
        -reducer "/bin/cat" \
        -input "$ASSIGNMENT_INPUT" \
        -output "$ASSIGNMENT_OUTPUT"

    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.name="Task2_Update_$ITERATION" \
        -D mapreduce.job.reduces=3 \
        -files "$MAPPER","$REDUCER" \
        -mapper "python3 $MAPPER update" \
        -reducer "python3 $REDUCER update" \
        -input "$ASSIGNMENT_OUTPUT" \
        -output "$UPDATE_OUTPUT"

    # just the small per-cluster result, not the trip data itself
    hadoop fs -cat "$UPDATE_OUTPUT"/part-* \
        | sort -t$'\t' -k1,1n \
        > "$ITERATION_RESULT"

    awk -F'\t' '
        {
            printf "Cluster %s: medoid=(%s, %s), points=%d, avg_dissimilarity=%.2f\n",
                   $1, $2, $3, $4, $5
        }
    ' "$ITERATION_RESULT"

    CHANGED_ASSIGNMENTS=$(
        awk -F'\t' '
            {
                total += $6
            }
            END {
                print total + 0
            }
        ' "$ITERATION_RESULT"
    )

    echo "Changed assignments: $CHANGED_ASSIGNMENTS"
    echo

    LAST_UPDATE="$UPDATE_OUTPUT"

    if [ "$CHANGED_ASSIGNMENTS" -eq 0 ]; then
        echo "Converged after $ITERATION iteration(s)."
        break
    fi

    if [ "$ITERATION" -eq "$MAX_ITERATIONS" ]; then
        echo "Maximum iteration limit reached."
        break
    fi

    awk -F'\t' '
        {
            print $1 "\t" $2 "\t" $3
        }
    ' "$ITERATION_RESULT" > "$CURRENT_MEDOIDS"

    PREVIOUS_ASSIGNMENT="$ASSIGNMENT_OUTPUT"

    # clean up the previous iteration's data, no longer needed
    if [ "$ITERATION" -gt 1 ]; then
        OLD_ITERATION=$((ITERATION - 1))

        hadoop fs -rm -r -f \
            "$WORK_ROOT/assignment_$OLD_ITERATION" \
            "$WORK_ROOT/update_$OLD_ITERATION"
    fi

    ITERATION=$((ITERATION + 1))
done

echo
echo "Creating final output in $OUTPUT ..."

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="Task2_Final_Output" \
    -D mapreduce.job.reduces=3 \
    -files "$MAPPER","$REDUCER" \
    -mapper "python3 $MAPPER final" \
    -reducer "python3 $REDUCER final" \
    -input "$LAST_UPDATE" \
    -output "$OUTPUT"

FINAL_COUNT=$(
    hadoop fs -cat "$OUTPUT"/part-* |
    awk 'END {print NR}'
)

if [ "$FINAL_COUNT" -ne "$K" ]; then
    echo "ERROR: Expected $K final clusters but produced $FINAL_COUNT."
    exit 1
fi

hadoop fs -rm -r -f "$WORK_ROOT"

echo
echo "Task 2 completed successfully."
echo "Final HDFS output: $OUTPUT"
echo "Final cluster count: $FINAL_COUNT"