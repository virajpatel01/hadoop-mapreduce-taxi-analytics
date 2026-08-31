# Hadoop MapReduce Taxi Analytics

A Python + Hadoop Streaming project for taxi trip, clustering, and fleet-performance analytics.

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
├── README.md
├── initialization.txt
├── sample_taxis.txt
├── sample_trips.txt
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

The original `Trips.txt` and `Taxis.txt` datasets used during development are **not redistributed in this repository**.

Instead, the repository includes two small, fully synthetic demonstration files:

```text
sample_trips.txt
sample_taxis.txt
```

They follow the same schemas expected by the MapReduce programs and can be used to test the project without access to the original data.

### `Trips.txt` schema

Each line contains 8 comma-separated fields:

```text
trip_id,taxi_id,fare,distance,pickup_x,pickup_y,dropoff_x,dropoff_y
```

Synthetic example:

```text
1,101,24.50,18.20,10.000,10.000,85.679,99.074
```

The project uses:

- `taxi_id` for taxi-level and company-level analysis
- `fare` for fare statistics and company revenue
- `distance` for trip classification and company distance metrics
- `dropoff_x` and `dropoff_y` for k-medoid clustering

### `Taxis.txt` schema

Each line contains 4 comma-separated fields:

```text
taxi_id,company_id,model,year
```

Synthetic example:

```text
101,0,42,2019
```

Task 3 uses `taxi_id` and `company_id` to join taxi information with trip records.

## Quick Demo with the Included Synthetic Data

The shell scripts expect these exact HDFS paths:

```text
/Input/Trips.txt
/Input/Taxis.txt
```

You can upload the bundled synthetic files directly under those names:

```bash
hadoop fs -mkdir -p /Input

hadoop fs -rm -f /Input/Trips.txt /Input/Taxis.txt

hadoop fs -put sample_trips.txt /Input/Trips.txt
hadoop fs -put sample_taxis.txt /Input/Taxis.txt
```

Then run:

```bash
chmod +x task1-run.sh task2-run.sh task3-run.sh

./task1-run.sh
./task2-run.sh
./task3-run.sh
```

Inspect the results:

```bash
hadoop fs -cat /Output/task1/part-*
hadoop fs -cat /Output/task2/part-*
hadoop fs -cat /Output/task3/part-*
```

The included synthetic trip data contains:

- 18 trips
- 6 taxis
- 3 companies
- short, medium, and long trips
- three drop-off clusters positioned around the medoids in the included `initialization.txt`

This makes the sample suitable for demonstrating all three workflows.

---

## Task 1 — Taxi-Level Trip Efficiency Analysis

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

## Task 2 — PAM / k-Medoid Clustering

Task 2 clusters taxi trip drop-off locations using the Partitioning Around Medoids (PAM) algorithm and Euclidean distance.

The iterative process performs:

1. assignment of each drop-off point to its nearest medoid
2. medoid update by evaluating candidate data points
3. convergence checking based on assignment changes

### `initialization.txt`

The first line specifies the maximum number of iterations. Every following line defines an initial medoid:

```text
maximum_iterations
medoid1_x medoid1_y
medoid2_x medoid2_y
...
```

The included file is:

```text
10
85.679 99.074
11.737 11.615
83.802 1.277
```

This represents:

```text
v = 10
k = 3
```

The synthetic sample data was intentionally created around these three medoid regions, so the included initialization file can be used directly for the demo.

Run:

```bash
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

## Task 3 — Company Performance Analysis

Task 3 is implemented as a three-job MapReduce pipeline.

### Job 1 — Join

Joins taxi/company information from `Taxis.txt` with trip information from `Trips.txt` using `taxi_id`.

### Job 2 — Aggregation

Calculates for each company:

- total revenue
- total trips
- fleet size
- revenue per taxi
- average trip distance

### Job 3 — Sorting

Sorts company results by total revenue in descending order.

Run:

```bash
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

The project was designed for a Hadoop environment with Hadoop Streaming available at:

```text
/usr/lib/hadoop-mapreduce/hadoop-streaming.jar
```

Requirements:

- Hadoop / HDFS
- Hadoop Streaming
- Python 3
- Bash

The Python implementation uses only the standard library.

All Hadoop jobs are configured to use three reducers.

## Using Your Own Data

To use another dataset, create files following the schemas above and upload them to:

```text
/Input/Trips.txt
/Input/Taxis.txt
```

The shell scripts can then be run without changing the source code.

## Notes

- The included sample records are synthetic and are provided only to demonstrate the expected data format and execution flow.
- The original development datasets are intentionally excluded.
- The shell scripts manage their required HDFS output paths automatically.
- Task 2 and Task 3 also manage intermediate HDFS outputs as part of their pipelines.

## Technologies

- Python
- Hadoop MapReduce
- Hadoop Streaming
- HDFS
- Bash
- Git

## Author

Viraj Patel
