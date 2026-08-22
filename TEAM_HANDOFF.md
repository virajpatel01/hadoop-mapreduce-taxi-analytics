# Big Data Processing A1 — Team Handoff

## Project

**RMIT COSC2637 Big Data Processing — Assignment 1 (2026)**  
**MapReduce-Based Taxi Fleet and Company Performance Analysis**

Repository:

```text
https://github.com/virajpatel01/Big-Data-Processing-A1.git
```

This file is the shared technical handoff for the group. Git is the source of truth for code; this document records the workflow, completed work, tested results, known fixes, and the next task.

---

## Development Workflow

- VS Code + macOS terminal
- Python 3 + Hadoop Streaming
- Python standard library only
- No `mrjob` or third-party Python packages
- Develop on task-specific branches; do not code directly on `main`

Branch model:

```text
main
├── task1
├── task2
└── task3
```

For each task:

1. Design the MapReduce data flow.
2. Implement mapper/reducer/shell script.
3. Run local syntax checks.
4. Test locally where practical.
5. Validate counts and output format.
6. Commit a checkpoint.
7. Test on RMIT EMR.
8. Inspect Hadoop counters and HDFS output.
9. Test rerun/cleanup behaviour.
10. Commit final fixes and merge after review.

Typical checks:

```bash
python3 -m py_compile mapper.py
python3 -m py_compile reducer.py
bash -n task*-run.sh
```

---

# Task 1 — COMPLETE

## Files

```text
task1_mapper.py
task1_reducer.py
task1-run.sh
```

Task 1 computes, for every `(taxi_id, trip_type)`:

- total trips
- maximum fare
- minimum fare
- average fare

Trip classification:

```text
long    distance >= 200
medium  100 <= distance < 200
short   distance < 100
```

The mapper uses **in-mapper combining** with state kept across input lines:

```text
(taxi_id, trip_type) -> [count, fare_sum, min_fare, max_fare]
```

The reducer combines partial statistics and emits exactly six tab-separated fields:

```text
taxi_id    trip_type    total_trips    max_fare    min_fare    average_fare
```

HDFS:

```text
Input:  /Input/Trips.txt
Output: /Output/task1
Reducers: 3
```

EMR verification on the supplied dataset:

```text
Input trip records: 30000
Final taxi/trip-type groups: 1436
Final trip-count total: 30000
Mapper output after in-mapper combining: 8557
Reducers: 3
```

The shell script was also rerun successfully with an existing output directory, confirming automatic HDFS cleanup.

---

# Task 2 — COMPLETE

## Files

```text
task2_mapper.py
task2_reducer.py
task2-run.sh
initialization.txt
```

Task 2 implements iterative **PAM / k-medoid clustering** of taxi trip drop-off coordinates using Euclidean distance. It must not be replaced by k-means centroids.

Supplied initialization:

```text
10
85.679 99.074
11.737 11.615
83.802 1.277
```

Meaning:

```text
v = 10
k = 3
cluster 0 -> (85.679, 99.074)
cluster 1 -> (11.737, 11.615)
cluster 2 -> (83.802, 1.277)
```

## Critical bug already fixed

The initial shell implementation used an uninitialised AWK variable for cluster IDs, causing cluster 0 to disappear on EMR. The correct code must explicitly start at zero:

```awk
BEGIN {
    cluster_id = 0
}
```

Do not remove this.

## Iterative pipeline

```text
Current medoids
      ↓
Assignment MapReduce job
      ↓
/tmp/task2_work/assignment_i
      ↓
PAM Update MapReduce job
      ↓
/tmp/task2_work/update_i
      ↓
print cluster statistics + count changed assignments
      ↓
changed == 0 ? stop : next iteration
```

All Task 2 Hadoop stages use 3 reducers.

Assignment mapper output:

```text
Key:   cluster_id
Value: trip_id    dropoff_x    dropoff_y    changed
```

The update mapper locally aggregates repeated coordinates using:

```text
(cluster_id, dropoff_x, dropoff_y) -> point_count, changed_count
```

For each cluster, the reducer evaluates every actual assigned coordinate as a medoid candidate. Weighted candidate cost:

```text
sum(frequency(point) * EuclideanDistance(candidate, point))
----------------------------------------------------------------
                       total cluster points
```

The lowest-cost actual data point becomes the new medoid.

Convergence sequence on the supplied dataset:

```text
Iteration 1 -> 30000 changed
Iteration 2 -> 3029 changed
Iteration 3 -> 314 changed
Iteration 4 -> 1234 changed
Iteration 5 -> 0 changed
```

Final converged output:

```text
51.323    78.236    14576    24.68
13.409    25.872     9181    20.85
70.175    17.378     6243    17.82
```

Final format:

```text
medoid_x    medoid_y    #points    average_dissimilarity
```

HDFS:

```text
Input:        /Input/Trips.txt
Intermediate: /tmp/task2_work/
Final output: /Output/task2
Reducers:     3 per job
```

EMR verification:

```text
Input records: 30000
Final clusters: 3
Final point-count total: 30000
Final fields per line: 4
Convergence iteration: 5
```

`hadoop fs -getmerge /Output/task2/part* task2_output.txt` was verified to produce exactly three lines. Temporary HDFS work directories are cleaned after success.

---

# Task 3 — NEXT TASK (COSC2637 Postgraduate)

Task 3 is worth 10 marks and must use **exactly three MapReduce jobs**:

```text
JOB 1 — JOIN
Trips.txt + Taxis.txt
        ↓
JOB 2 — AGGREGATION
Company performance metrics
        ↓
JOB 3 — SORTING
Total revenue descending
```

Required final metrics per company:

1. company
2. total revenue
3. total trips
4. fleet size (distinct taxis)
5. revenue per taxi
6. average trip distance

Definitions:

```text
Total revenue = sum of company fares
Total trips = number of company trips
Fleet size = number of DISTINCT taxis belonging to the company
Revenue per taxi = total revenue / fleet size
Average trip distance = total trip distance / total trips
```

Inputs:

```text
/Input/Trips.txt
/Input/Taxis.txt
```

Final output:

```text
/Output/task3
```

Required shell script:

```text
task3-run.sh
```

All three jobs must use exactly 3 reducers.

## Critical Task 3 constraints

- Final companies must be ordered by **total revenue descending**.
- Sorting must happen **inside MapReduce**.
- Do not use Bash `sort` to create the assessed final ordering.
- `TotalOrderPartitioner` is prohibited.
- The implementation must work on larger/different marker datasets.

Before coding Task 3, design these points explicitly:

- reduce-side join key/value structure
- tagging Trips vs Taxis records
- taxi-to-company mapping
- company-level aggregation
- distinct taxi counting / fleet size
- revenue and trip-distance accumulators
- global descending revenue ordering with 3 reducers
- intermediate HDFS paths
- exact final output format
- shell-script chaining and cleanup

Do not jump straight into implementation before the three-job design is settled.

---

# RMIT EMR Workflow

Connection path:

```text
Mac -> jumphost -> EMR
```

From Mac:

```bash
ssh-add ~/Downloads/s4126405-cosc2637.pem
ssh -A jumphost
```

From jumphost:

```bash
ssh hadoop@<CURRENT_EMR_PRIVATE_IP>
```

The EMR private IP changes when a new cluster is created. Do not blindly reuse an old IP.

For a fresh cluster:

```bash
hadoop fs -mkdir -p /Input
hadoop fs -put Trips.txt /Input/
hadoop fs -put Taxis.txt /Input/
```

Hadoop Streaming JAR:

```text
/usr/lib/hadoop-mapreduce/hadoop-streaming.jar
```

---

# Worklog

The official worklog already contains the technical explanations for Task 1 and Task 2.

After Task 3 is finished, add its explanation covering:

- data flow
- mapper/reducer key-value pairs
- join strategy
- aggregation strategy
- distinct taxi strategy
- partitioner/comparator behaviour
- movement between all three MapReduce jobs
- sorting design
- HDFS paths
- EMR verification

Section 1 must use real dates, contributors, and time spent. Do not fabricate worklog entries.

---

# Submission Discipline

Before final submission:

- all code must run on RMIT EMR
- required HDFS paths must exactly match the specification
- shell scripts must have the required names
- Python/shell files must be the latest tested versions
- submission ZIP must contain no subfolders
- worklog must be completed and submitted separately
- code PDF must be submitted separately
- filenames must follow the assignment naming convention
- expect the marker to test larger/different data

**Git is the source of truth for implementation history. Chat conversations are supporting context only.**
