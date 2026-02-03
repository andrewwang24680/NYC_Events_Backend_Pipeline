# NYC_Events_Backend_Pipeline
Distributed backend ETL service built on MapReduce with deterministic output contracts and Hive‑ready storage.


End‑to‑end backend project that cleans and profiles NYC permitted events data at scale with Hadoop MapReduce, then enriches each event with a taxi zone `LocationID` using the official NYC Taxi Zones lookup. Outputs are schema‑stable CSV and partitioned ORC tables for analytics. This repository covers the events portion of a larger group project; other team members analyze taxi, subway, and Citi Bike datasets.

## Impact
- Built a distributed MapReduce pipeline with deterministic outputs and an explicit data contract.
- Implemented resilient ingestion (CSV parsing + multi‑format timestamp normalization) with strict validation and fail‑fast filters.
- Added lookup‑based enrichment using a cached reference dataset and longest‑match string mapping.
- Shipped analytics‑ready storage with partitioned ORC tables in Hive for query performance.
- Maintained clear boundaries between batch cleaning, profiling, and downstream analytics.

## Tech Stack
Java, Hadoop MapReduce, HDFS, Hive, ORC

## Technical Approach
The cleaning job reads raw events CSVs and applies:
- Required field validation (`event_id`, `event_name`, `start_time`, `end_time`).
- Timestamp normalization across multiple input formats, emitting a single `yyyy-MM-dd HH:mm:ss` format.
- Record filtering when `start_time > end_time` or timestamps fail to parse.
- Taxi zone enrichment by loading `NYC_Taxi_Zones.csv` via DistributedCache, standardizing zone names and event locations (uppercase, remove punctuation and trailing numeric suffixes, collapse spaces), then matching by longest substring containment. Only numeric `LocationID`s are accepted.

Output schema:
`event_id,event_name,start_ts,end_ts,event_type,start_location_id,end_location_id`

## Profiling Job
The profiling job scans raw events to quantify data quality and distribution, including:
- Total row count and malformed rows.
- Missing required fields (IDs, names, timestamps, type, location).
- Invalid timestamps based on expected input format.
- Counts per event type and per raw location string.
- Min/max start and end timestamps for range checks.

## How to Run (Local or Dataproc)
Build and run the cleaner:
- Compile and package:
  - `javac -classpath "$(hadoop classpath)" events/*.java`
  - `jar cf events-mr.jar events/*.class`
- Run:
  - `hadoop jar events-mr.jar events.EventsCleaner <input_path> <output_path> <taxi_zones_path>`

Run the profiler:
- `hadoop jar events-mr.jar events.EventsProfiler <input_path> <output_path>`

## Hive ORC Conversion
Use `convert_to_orc_partitioned.txt` for partitioned ORC creation (year/month) or
`create_matched_orc_partitioned.hql` to filter for matched `LocationID`s only.

## Repository Structure
- `src/main/java/events`: canonical Java sources.
- `dataproc-upload/`: simplified upload bundle for cluster execution.
- `scripts/`: helper scripts for running jobs.
- `convert_to_orc_partitioned.txt`: Hive DDL/DML for partitioned ORC.
