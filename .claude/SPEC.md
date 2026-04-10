# Earthquake Analytics ETL Pipeline — Spec Document

## 1. Overview

A daily batch ETL pipeline that ingests global earthquake data from the USGS Earthquake API, processes it through a medallion architecture (Bronze → Silver → Gold), and serves it to an interactive Streamlit dashboard.

The pipeline runs on **Databricks** using PySpark notebooks orchestrated by a Databricks Asset Bundle (DAB) job. Data is stored in **Delta Lake** with Change Data Feed (CDF) enabled for incremental processing between layers.

---

## 2. Architecture

```
USGS API
   │
   ▼
[Bronze] bronze_events         ← raw GeoJSON, MapType columns
   │  (CDF enabled)
   ▼
[Silver] silver_events         ← typed, enriched, deduplicated
   │  (CDF enabled)
   ▼
[Gold] gold_events_map         ← denormalized for map display
       gold_daily_summary      ← daily aggregations
       gold_regional_summary   ← regional aggregations
   │
   ▼
Streamlit Dashboard (Databricks App)
```

![Architecture](architecture.png)

---

## 3. Repository Structure

```
earthquake_analytics/
├── databricks.yml                          # DAB bundle definition
├── resources/
│   └── earthquake_analytics_etl_job.job.yml  # Job + task definitions
├── etl/
│   ├── 01_bronze_ingestion.ipynb           # Bronze layer notebook
│   ├── 02_silver_transformation.ipynb      # Silver layer notebook
│   └── 03_gold_aggregation.ipynb           # Gold layer notebook
├── utils/
│   ├── datasource.py                       # Custom USGS PySpark DataSource
│   ├── helpers.py                          # Shared Delta Lake utilities
│   ├── main.py                             # Wheel entry point
│   └── __init__.py
├── streamlit_app/
│   ├── app.py                              # Dashboard application
│   ├── app.yaml                            # Databricks App config
│   └── requirements.txt
├── dist/
│   └── utils-0.0.1-py3-none-any.whl       # Built wheel (deployed to cluster)
└── pyproject.toml
```

---

## 4. Infrastructure & Deployment

### Databricks Asset Bundle (DAB)

| Setting | Value |
|---|---|
| Bundle name | `earthquake_analytics` |
| Workspace | `https://dbc-678a5d85-cca0.cloud.databricks.com/` |

### Environments

| Target | Catalog | Schema | Notes |
|---|---|---|---|
| `dev` | `quake_lake` | `usgs_earthquakes_dev` | Default; resources prefixed with `[dev username]`; schedules paused |
| `prod` | `quake_lake` | `usgs_earthquakes_prod` | Deployed to `/Workspace/Users/admin@pathfinder-analytics.net` |

### Job Schedule

Runs **daily** (periodic interval: 1 day).

### Python Wheel

The `utils` package is built via `uv build --wheel` and deployed as `dist/utils-0.0.1-py3-none-any.whl`. This wheel is installed on the cluster as a job environment dependency before any notebook tasks run.

---

## 5. Job Parameters

| Parameter | Default | Description |
|---|---|---|
| `catalog` | `${var.catalog}` | Unity Catalog name |
| `schema` | `${var.schema}` | Schema / database name |
| `start_date` | `""` | Ingestion start date (YYYY-MM-DD). Empty = yesterday |
| `end_date` | `""` | Ingestion end date (YYYY-MM-DD). Empty = today |
| `num_partitions` | `"1"` | Number of time-range partitions for parallel USGS API fetching |

---

## 6. Task Dependency Chain

```
python_wheel_task          ← installs utils wheel, validates entry point
        │
        ▼
bronze_ingestion           ← 01_bronze_ingestion.ipynb
        │
        ▼
silver_transformation      ← 02_silver_transformation.ipynb
        │
        ▼
gold_aggregation           ← 03_gold_aggregation.ipynb
```

---

## 7. Data Source — USGS Earthquake API

**Endpoint:** `https://earthquake.usgs.gov/fdsnws/event/1/query`

**Format:** GeoJSON FeatureCollection

**Custom PySpark DataSource** (`utils/datasource.py`):

- Registered as `spark.read.format("usgs")`
- Splits the date range into `numPartitions` equal sub-ranges, each fetched in parallel as a Spark partition
- Each partition makes one HTTP GET request (30s timeout)
- Returns rows with schema:

| Column | Type | Description |
|---|---|---|
| `type` | StringType | GeoJSON feature type (always `"Feature"`) |
| `properties` | `MapType(String, String)` | All USGS event properties |
| `geometry` | `MapType(String, String)` | GeoJSON geometry (type + coordinates) |
| `id` | StringType | Unique USGS event ID |

**Options:**

| Option | Required | Description |
|---|---|---|
| `starttime` | Yes | ISO8601 or YYYY-MM-DD |
| `endtime` | Yes | ISO8601 or YYYY-MM-DD; must be after starttime |
| `numPartitions` | No | Default `10`; splits date range for parallelism |

---

## 8. Delta Lake Tables

All tables live at `{catalog}.{schema}.{table}` and use **Delta Lake** format.

### A. bronze_events

Raw ingestion table. Schema mirrors the USGS DataSource output with added metadata.

| Column | Type | Notes |
|---|---|---|
| `type` | STRING | GeoJSON feature type |
| `properties` | MAP<STRING, STRING> | All USGS event properties (unparsed) |
| `geometry` | MAP<STRING, STRING> | GeoJSON geometry |
| `id` | STRING | USGS event ID — merge key |
| `_ingested_at` | TIMESTAMP | When the record was ingested |
| `_source` | STRING | Always `"usgs_api"` |

- **CDF enabled:** Yes
- **Write mode:** `merge` on `id` (idempotent re-runs)

---

### B. silver_events

Typed and enriched events. Extracted from bronze MapType columns.

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | USGS event ID — merge key |
| `magnitude` | DOUBLE | Richter magnitude |
| `magnitude_type` | STRING | e.g. `"ml"`, `"mw"` |
| `magnitude_category` | STRING | Derived: micro/minor/light/moderate/strong/major/great |
| `place` | STRING | Human-readable location description |
| `region` | STRING | Derived: extracted from place (after " of ") |
| `event_time` | TIMESTAMP | UTC event time (converted from ms epoch) |
| `updated_time` | TIMESTAMP | Last USGS update time |
| `timezone_offset` | INTEGER | Minutes offset from UTC |
| `latitude` | DOUBLE | From geometry coordinates[1] |
| `longitude` | DOUBLE | From geometry coordinates[0] |
| `depth_km` | DOUBLE | From geometry coordinates[2] |
| `depth_category` | STRING | Derived: shallow (<70km) / intermediate (<300km) / deep |
| `rms` | DOUBLE | Root mean square travel time residual |
| `gap` | DOUBLE | Largest azimuthal gap between stations |
| `dmin` | DOUBLE | Horizontal distance to nearest station |
| `station_count` | INTEGER | Number of stations used |
| `significance` | INTEGER | USGS significance score (0–1000+) |
| `felt_reports` | INTEGER | Number of "Did You Feel It?" reports |
| `cdi` | DOUBLE | Community Internet Intensity |
| `mmi` | DOUBLE | Modified Mercalli Intensity |
| `alert_level` | STRING | PAGER alert: green/yellow/orange/red |
| `alert_level_numeric` | INTEGER | Derived: 0=none, 1=green, 2=yellow, 3=orange, 4=red |
| `tsunami_flag` | INTEGER | 1 if in oceanic region; does not confirm tsunami |
| `has_tsunami_warning` | BOOLEAN | Derived: `tsunami_flag == 1` |
| `is_reviewed` | BOOLEAN | Derived: `review_status == "reviewed"` |
| `is_significant` | BOOLEAN | Derived: `significance >= 500` |
| `network` | STRING | Contributing network code |
| `event_code` | STRING | Event code within the network |
| `review_status` | STRING | `"automatic"` or `"reviewed"` |
| `detail_url` | STRING | USGS event detail page |
| `api_detail_url` | STRING | USGS GeoJSON detail endpoint |
| `_processed_at` | TIMESTAMP | When this record was silver-processed |

- **CDF enabled:** Yes
- **Write mode:** `merge` on `event_id`
- **Data quality filters:** drops rows with null `event_id`, `magnitude`, `latitude`, `longitude`, or `event_time`
- **Deduplication:** window function on `event_id` ordered by `updated_time DESC`, keeps latest

---

### C. gold_events_map

Denormalized view for individual event map display. One row per earthquake.

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | Merge key |
| `latitude` | DOUBLE | |
| `longitude` | DOUBLE | |
| `place` | STRING | |
| `region` | STRING | |
| `magnitude` | DOUBLE | |
| `magnitude_category` | STRING | |
| `significance` | INTEGER | Primary sizing/coloring metric |
| `size_factor` | DOUBLE | `significance / 100.0` — for map radius scaling |
| `depth_km` | DOUBLE | |
| `depth_category` | STRING | |
| `event_time` | TIMESTAMP | |
| `alert_level` | STRING | |
| `has_tsunami_warning` | BOOLEAN | |
| `felt_reports` | INTEGER | |
| `severity` | STRING | Derived from significance: low/minor/moderate/major/severe |
| `detail_url` | STRING | |
| `_processed_at` | TIMESTAMP | |

Severity thresholds:

| Severity | Significance Range |
|---|---|
| severe | >= 600 |
| major | 400–599 |
| moderate | 200–399 |
| minor | 100–199 |
| low | < 100 |

- **CDF enabled:** No (end of pipeline)
- **Write mode:** `merge` on `event_id`

---

### D. gold_daily_summary

One row per calendar day.

| Column | Type | Notes |
|---|---|---|
| `date` | TIMESTAMP | Truncated to day — merge key |
| `total_events` | LONG | |
| `high_significance_events` | LONG | significance >= 500 |
| `tsunami_warnings` | LONG | |
| `avg_significance` | DOUBLE | Rounded to 0 decimal |
| `max_significance` | INTEGER | |
| `min_significance` | INTEGER | |
| `avg_magnitude` | DOUBLE | Rounded to 2 decimal |
| `max_magnitude` | DOUBLE | |
| `avg_depth_km` | DOUBLE | Rounded to 2 decimal |
| `total_felt_reports` | LONG | |
| `count_severe` | LONG | significance >= 600 |
| `count_major` | LONG | 400–599 |
| `count_moderate` | LONG | 200–399 |
| `count_minor` | LONG | 100–199 |
| `count_low` | LONG | < 100 |
| `_processed_at` | TIMESTAMP | |

- **Write mode:** `merge` on `date`
- **Incremental:** only recomputes rows for affected dates when processing incrementally

---

### E. gold_regional_summary

One row per region (extracted from USGS place field).

| Column | Type | Notes |
|---|---|---|
| `region` | STRING | Merge key; null rows excluded |
| `total_events` | LONG | |
| `high_significance_events` | LONG | significance >= 500 |
| `avg_significance` | DOUBLE | Rounded to 0 decimal |
| `max_significance` | INTEGER | |
| `avg_magnitude` | DOUBLE | Rounded to 2 decimal |
| `max_magnitude` | DOUBLE | |
| `avg_depth_km` | DOUBLE | Rounded to 2 decimal |
| `first_event` | TIMESTAMP | |
| `last_event` | TIMESTAMP | |
| `centroid_lat` | DOUBLE | Average latitude, rounded to 4 decimal |
| `centroid_lon` | DOUBLE | Average longitude, rounded to 4 decimal |
| `_processed_at` | TIMESTAMP | |

- **Write mode:** `merge` on `region`
- **Incremental:** only recomputes rows for affected regions

---

### F. _checkpoints (internal)

Tracks the last processed Delta table version per layer transition (bronze→silver, silver→gold).

| Column | Type |
|---|---|
| `source_table` | STRING |
| `processed_version` | INTEGER |
| `records_processed` | INTEGER |
| `processed_at` | TIMESTAMP |

- **Write mode:** `append`
- Used by `read_incremental_or_full()` to determine whether to use CDF or fall back to full read

---

## 9. Incremental Processing Pattern

All inter-layer reads use this pattern (`utils/helpers.py: read_incremental_or_full`):

1. Get current version of source table via `DESCRIBE HISTORY`
2. Look up last processed version from `_checkpoints`
3. **If checkpoint exists and is behind:** read CDF changes (`startingVersion = last + 1`); filter to `insert` and `update_postimage` only
4. **If checkpoint is current:** return empty DataFrame (no-op)
5. **If no checkpoint:** full table read (first run)
6. After successful write, call `save_checkpoint()` to record the new version

This ensures downstream notebooks only process rows that actually changed, making the pipeline efficient for daily incremental loads.

---

## 10. Utility Functions (`utils/helpers.py`)

| Function | Description |
|---|---|
| `get_or_create_catalog_schema(spark, catalog, schema)` | Creates catalog and schema if not exist |
| `get_table_path(catalog, schema, table)` | Returns `catalog.schema.table` string |
| `table_exists(spark, table_path)` | Returns bool; uses `DESCRIBE TABLE` |
| `write_delta_table(df, table_path, mode, merge_keys, partition_by)` | Write with merge/overwrite/append |
| `write_delta_table_with_cdf(df, table_path, mode, merge_keys, enable_cdf)` | Same + CDF property on creation |
| `add_metadata_columns(df)` | Adds `_ingested_at`, `_source` columns |
| `read_incremental_or_full(spark, source_table, checkpoint_table)` | CDF incremental read with fallback |
| `read_cdf_changes(spark, table_path, start_version, end_version)` | Raw CDF reader |
| `save_checkpoint(spark, checkpoint_table, source_table, version, count)` | Appends checkpoint record |
| `get_latest_processed_version(spark, checkpoint_table, source_table)` | Reads max version from checkpoints |
| `get_current_table_version(spark, table_path)` | Reads latest version from Delta history |
| `delete_date_range(spark, table_path, date_column, start, end)` | Deletes a date range for reprocessing |
| `print_table_stats(spark, table_path)` | Prints record count, last modified, last operation |

---

## 11. Streamlit Dashboard (`streamlit_app/app.py`)

Deployed as a **Databricks App** (`earthquake-analytics-app`).

**Environment variables required:**

| Variable | Description |
|---|---|
| `CATALOG` | Unity Catalog name |
| `SCHEMA` | Schema name |
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID for queries |

**Features:**

- Interactive sidebar filters: time range (Last Day / Week / Month) and minimum significance slider (0–1000)
- 3D ColumnLayer map (pydeck): column height = significance × 100; color = severity tier
- Line chart: daily event count over selected period
- Bar chart: top 10 regions by event count
- Table: high-significance events (500+) with time, significance, magnitude, place, depth, alert level
- 5-minute query cache (`@st.cache_data(ttl=300)`)

---

## 12. Derived Column Reference

### Magnitude Category

| Category | Range |
|---|---|
| great | >= 8.0 |
| major | >= 7.0 |
| strong | >= 6.0 |
| moderate | >= 5.0 |
| light | >= 4.0 |
| minor | >= 2.0 |
| micro | < 2.0 |

### Depth Category

| Category | Depth |
|---|---|
| shallow | < 70 km |
| intermediate | 70–299 km |
| deep | >= 300 km |

### Severity (gold_events_map)

| Severity | Significance | Color |
|---|---|---|
| severe | >= 600 | Red `[255,0,0]` |
| major | >= 400 | Orange `[255,127,0]` |
| moderate | >= 200 | Yellow `[255,255,0]` |
| minor | >= 100 | Green `[0,255,0]` |
| low | < 100 | Cyan `[0,200,255]` |

---

## 13. Dependencies

### Cluster / Job Environment

- PySpark (Databricks Runtime)
- `delta-spark` (included in DBR)
- `requests` (for USGS API calls)
- `utils-0.0.1-py3-none-any.whl` (built from this repo)

### Streamlit App

See `streamlit_app/requirements.txt`:
- `streamlit`
- `pydeck`
- `pandas`
- `databricks-sql-connector`
- `databricks-sdk`

### Dev / Build

- `uv` (package manager)
- `pyproject.toml` (project metadata)

---

## 14. Notebook Widget Parameters

### 01_bronze_ingestion.ipynb

| Widget | Type | Default | Description |
|---|---|---|---|
| `catalog` | text | `earthquakes_dev` | Target catalog |
| `schema` | text | `usgs` | Target schema |
| `start_date` | text | `""` (yesterday) | Ingestion window start |
| `end_date` | text | `""` (today) | Ingestion window end |
| `num_partitions` | text | `"8"` | USGS API fetch parallelism |
| `write_mode` | dropdown | `merge` | merge / overwrite / append |

### 02_silver_transformation.ipynb

| Widget | Type | Default | Description |
|---|---|---|---|
| `catalog` | text | `earthquakes_dev` | Source/target catalog |
| `schema` | text | `usgs` | Source/target schema |
| `write_mode` | dropdown | `merge` | merge / overwrite |

### 03_gold_aggregation.ipynb

| Widget | Type | Default | Description |
|---|---|---|---|
| `catalog` | text | `earthquakes_dev` | Source/target catalog |
| `schema` | text | `usgs` | Source/target schema |

---

## 15. Idempotency & Re-run Safety

- **Bronze:** merge on USGS `id`; re-ingesting the same date range will upsert, not duplicate
- **Silver:** merge on `event_id`; deduplication window ensures one row per event (latest update wins)
- **Gold:** merge on `event_id` / `date` / `region`; safe to re-run
- **Checkpoints:** `save_checkpoint` is append-only; `get_latest_processed_version` takes `max`, so duplicate checkpoint rows are harmless

---

## 16. Definition of Done

- [ ] `python_wheel_task` deploys `utils` wheel successfully
- [ ] `bronze_events` table is created with CDF enabled; re-runs merge without duplicating
- [ ] `silver_events` table contains typed, enriched, deduplicated records
- [ ] `gold_events_map`, `gold_daily_summary`, `gold_regional_summary` are populated
- [ ] Incremental runs (day 2+) only process changed records via CDF
- [ ] Checkpoints advance correctly after each successful layer write
- [ ] Streamlit dashboard loads and displays map, charts, and metrics
- [ ] Job runs on daily schedule without errors in both dev and prod targets
