# spatial_knn
A compilation of solutions for the KNN (K-Nearest Neighbours) problem applied to spatial data.

See the notebook for a speed comparison across methods.

## Local Setup

A PostgreSQL + PostGIS instance is provided via Docker for running the SQL-based tests locally.

**Requirements:** Docker and Docker Compose.

1. Copy the environment file and adjust credentials if needed:
   ```bash
   cp .env.example .env
   ```
2. Start the database:
   ```bash
   docker compose up -d
   ```
3. Load data into the database (**one-time setup** — data is stored in a named Docker volume and persists across restarts). Use whichever source you have available:
   - **From Parquet files** (preprocessed, no raw data needed):
     ```bash
     uv run --env-file .env prepare_data.py --from-parquet
     ```
   - **From raw CSV/GPKG files** (transforms, writes Parquet + CSV, loads PostGIS):
     ```bash
     uv run --env-file .env prepare_data.py
     ```

### Raw data requirements

If running `--from-raw`, download the following Ordnance Survey open datasets and place them under `data/raw/`:

| File | Source | Description |
|---|---|---|
| `data/raw/osopenuprn_<date>.csv` | [OS Open UPRN](https://osdatahub.os.uk/downloads/open/OpenUPRN) | Unique Property Reference Numbers with coordinates (full GB) |
| `data/raw/codepo_gb.gpkg` | [Code-Point Open](https://osdatahub.os.uk/downloads/open/CodePointOpen) | Postcode centroids (GeoPackage format) |
| `data/raw/bdline_gb.gpkg` | [Boundary-Line](https://osdatahub.os.uk/downloads/open/BoundaryLine) | Administrative boundaries incl. district polygons (GeoPackage format) |

All three are free to download from the [OS Data Hub](https://osdatahub.os.uk) (account required).

## Running the benchmarks

`main.py` is a Jupyter-style notebook (using `# %%` cell markers). Open it in VS Code and run cells individually, or execute it top to bottom to benchmark all implementations and generate `results.png`.

It runs benchmarks for two datasets defined in the `SCENARIOS` list at the top of the file:

| Dataset | UPRN table | Codepoint table |
|---|---|---|
| White Horse (small) | `os.open_uprn_white_horse` | `os.code_point_open_white_horse` |
| Full GB (large) | `os.os_open_uprn` | `os.codepoint_polygons` |

Both scenarios are run automatically in sequence when you run all cells top to bottom. `main.py` can also be run directly from the command line:

```bash
# Run all scenarios
uv run --env-file .env main.py

# Run a single scenario
uv run --env-file .env main.py --scenario "White Horse (small)"
uv run --env-file .env main.py --scenario "Full GB (large)"
```

To run only one scenario in notebook mode, comment out the other entry in `SCENARIOS`.

Individual scripts can also be run directly. Each accepts `--uprn-table` and `--codepoint-table` arguments (defaulting to the White Horse dataset if omitted):

```bash
# White Horse (default)
uv run --env-file .env python/geopandas/knn.py

# Full GB
uv run --env-file .env python/geopandas/knn.py \
  --uprn-table os.os_open_uprn \
  --codepoint-table os.codepoint_polygons
```

## When to use what

- **SedonaDB** — data fits in memory, you want SQL semantics with Sedona's spatial functions without a server; Very fast, simple to use.
- **Shapely / Geopandas** — data fits in memory, Python-only stack, geometries beyond points.
- **DuckDB** — data fits in memory, you want SQL semantics without a server, or you are already working with Parquet files.
- **Scikit-Learn** — data fits in memory, points only. Could be faster than Shapely when finding only one neighbour per point, but cannot break ties by postcode (or any secondary sort key), so results may differ from the other implementations in those edge cases. It is not as flexible as the other python solutions so it might be used in particular cases.
- **SQL (PostgreSQL + PostGIS)** — data does not fit in memory, or you need to join against other tables and write complex queries.
- **C# (.NET / NetTopologySuite)** — already in the .NET ecosystem; API mirrors the JVM JTS library. Similar speed than the JVM solutions, easy to write and easier to run as it requires less settings.
- **Scala / Kotlin (JVM)** — KNN is one part of a larger JVM application; slower than Go/Rust but with a larger library ecosystem.
- **Go / Rust** — maximum single-machine speed; choose when the compiled language fits your stack. Rust edges out Go slightly. This require more coding knowledge.
- **Apache Sedona / PySpark** — data is too large for a single machine and must be distributed across a cluster.
- **Databricks, BigQuery / Redshift / Snowflake / Athena** *(paid managed services)* — data lives in a cloud warehouse and you want to avoid moving it. BigQuery is the fastest of the four here; Athena is slowest and roughly on par with local Postgres for data that fits Postgres.

<!-- RESULTS_START -->
## Results

| test                           | time                   |
|:-------------------------------|:-----------------------|
| Go strtree                     | 0 days 00:00:00.321024 |
| rust strtree                   | 0 days 00:00:00.346612 |
| SedonaDB                       | 0 days 00:00:00.750684 |
| Go all vs all                  | 0 days 00:00:00.916740 |
| Shapely strtree                | 0 days 00:00:01.319851 |
| Geopandas sjoin_nearest        | 0 days 00:00:01.791905 |
| BigQuery                       | 0 days 00:00:03        |
| kotlin strtree                 | 0 days 00:00:03.627000 |
| rust all vs all                | 0 days 00:00:04.044441 |
| scala strtree                  | 0 days 00:00:04.142000 |
| C# strtree                     | 0 days 00:00:05.894800 |
| Apache Sedona st_knn           | 0 days 00:00:09.269729 |
| DuckDB                         | 0 days 00:00:17.465032 |
| C# all vs all                  | 0 days 00:00:22.904955 |
| Scikit-Learn nearest neighbour | 0 days 00:00:25.355890 |
| RedShift                       | 0 days 00:00:26        |
| scala all vs all               | 0 days 00:00:27.585000 |
| kotlin all vs all              | 0 days 00:00:32.706000 |
| Snowflake h3                   | 0 days 00:00:45        |
| Databricks pure sql            | 60s                    |
| SQL lateral                    | 63s                    |
| Snowflake cartesian            | 75s                    |
| Athena                         | 110s                   |
| Shapely all vs all             | 127s                   |
| SQL distinct                   | 130s                   |
| Apache Sedona partial sql      | 152s                   |
| Apache Sedona pure sql         | 158s                   |
| BigQuery (Slot time consumed)  | 310s                   |
<!-- RESULTS_END -->

![Benchmark results](results.png)
