# Hadoop MapReduce Taxi Analytics

A Python + Hadoop Streaming project for analysing taxi trip and fleet data with multiple MapReduce workflows.

The project demonstrates:

- in-mapper combining
- distributed aggregation
- PAM / k-medoids clustering
- reduce-side joins
- company-level performance analysis
- multi-job MapReduce pipelines
- distributed sorting with multiple reducers

## Project Structure

```text
.
├── initialization.txt
│
├── task1-run.sh
├── task1_mapper.py
├── task1_reducer.py
│
├── task2-run.sh
├── task2_mapper.py
├── task2_reducer.py
│
├── task3-run.sh
├── task3_compute_cutoffs.py
├── task3_job1_mapper.py
├── task3_job1_reducer.py
├── task3_job2_mapper.py
├── task3_job2_reducer.py
├── task3_job3_mapper.py
└── task3_job3_reducer.py
```

## Dataset

The original `Trips.txt` and `Taxis.txt` datasets are **not included in this repository**.

To run the project, provide your own CSV files that follow the expected schemas below.

### `Trips.txt`

Each line must contain 8 comma-separated fields:

```text
trip_id,taxi_id,fare,distance,pickup_x,pickup_y,dropoff_x,dropoff_y
```

Example:

```text
0,470,117.32,64.27,54.736,91.185,20.488,93.348
```

The project uses:

- `taxi_id` for taxi-level and company-level analysis
- `fare` for revenue and fare statistics
- `distance` for trip classification and company distance metrics
- `dropoff_x` and `dropoff_y` for k-medoid clustering

### `Taxis.txt`

Each line must contain 4 comma-separated fields:

```text
taxi_id,company_id,<field3>,<field4>
```

Example format:

```text
470,0,80,2018
```

For the current MapReduce implementation, Task 3 uses the first two fields:

```text
taxi_id
company_id
```

The remaining two fields are not required by the current analysis.

## HDFS Input Setup

The scripts expect the datasets at these exact HDFS paths:

```text
/Input/Trips.txt
/Input/Taxis.txt
```

Example setup:

```bash
hadoop fs -mkdir -p /Input

hadoop fs -put Trips.txt /Input/Trips.txt
hadoop fs -put Taxis.txt /Input/Taxis.txt
```

If the files already exist, remove or replace them before uploading new versions.

---

# Task 1 — Taxi-Level Trip Efficiency Analysis

Task 1 groups trips by:

```text
(taxi_id, trip_type)
```

Trip types are defined as:

```text
short   distance < 100
medium  100 <= distance < 200
long    distance >= 200
```

For each taxi and trip type, it calculates:

- total trips
- maximum fare
- minimum fare
- average fare

The mapper uses **in-mapper combining**, preserving aggregation state across input lines to reduce intermediate mapper output.

Run:

```bash
chmod +x task1-run.sh
./task1-run.sh
```

HDFS output:

```text
/Output/task1
```

Final output format:

```text
taxi_id    trip_type    total_trips    max_fare    min_fare    average_fare
```

---

# Task 2 — PAM / k-Medoid Clustering

Task 2 clusters taxi trip drop-off locations using the Partitioning Around Medoids (PAM) algorithm and Euclidean distance.

The algorithm repeatedly performs:

1. assignment of each drop-off point to its nearest medoid
2. medoid update by evaluating candidate points
3. convergence checking based on assignment changes

## `initialization.txt`

The first line specifies the maximum number of iterations.

Every following line defines one initial medoid:

```text
maximum_iterations
medoid1_x medoid1_y
medoid2_x medoid2_y
...
```

Example:

```text
10
85.679 99.074
11.737 11.615
83.802 1.277
```

This example represents:

```text
v = 10
k = 3
```

The implementation supports different values of `k` and `v`.

Run:

```bash
chmod +x task2-run.sh
./task2-run.sh
```

HDFS output:

```text
/Output/task2
```

Final output contains one line per cluster:

```text
medoid_x    medoid_y    number_of_points    average_dissimilarity
```

---

# Task 3 — Company Performance Analysis

Task 3 is implemented as a three-job MapReduce pipeline.

## Job 1 — Join

Joins taxi/company information from `Taxis.txt` with trip information from `Trips.txt` using `taxi_id`.

## Job 2 — Aggregation

Calculates the following metrics for each company:

- total revenue
- total trips
- fleet size
- revenue per taxi
- average trip distance

## Job 3 — Sorting

Sorts company results by total revenue in descending order.

Run:

```bash
chmod +x task3-run.sh
./task3-run.sh
```

HDFS output:

```text
/Output/task3
```

Final output format:

```text
company_id    total_revenue    total_trips    fleet_size    revenue_per_taxi    average_trip_distance
```

---

## Requirements

This project was designed for a Hadoop environment with Hadoop Streaming available at:

```text
/usr/lib/hadoop-mapreduce/hadoop-streaming.jar
```

You will need:

- Hadoop / HDFS
- Hadoop Streaming
- Python 3
- Bash

The implementation uses only the Python standard library.

All Hadoop jobs are configured to use three reducers.

## Running the Project

Place the Python files, shell scripts, and `initialization.txt` in the same working directory.

Upload compatible datasets to HDFS:

```bash
hadoop fs -mkdir -p /Input
hadoop fs -put Trips.txt /Input/Trips.txt
hadoop fs -put Taxis.txt /Input/Taxis.txt
```

Then execute any task:

```bash
./task1-run.sh
./task2-run.sh
./task3-run.sh
```

Inspect outputs with:

```bash
hadoop fs -cat /Output/task1/part-*
hadoop fs -cat /Output/task2/part-*
hadoop fs -cat /Output/task3/part-*
```

## Notes

- The repository intentionally does not redistribute the original taxi datasets.
- Users can run the project with their own compatible CSV data.
- The shell scripts automatically remove existing task output directories before launching new jobs.
- Intermediate Task 2 and Task 3 HDFS outputs are managed by their respective run scripts.

## Technologies

- Python
- Hadoop MapReduce
- Hadoop Streaming
- HDFS
- Bash
- Git

## Author

Viraj Patel
