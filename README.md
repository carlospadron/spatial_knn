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
   - **From ORC files** (preprocessed, faster):
     ```bash
     uv run --env-file .env load_orc_to_pg.py
     ```
   - **From raw CSV files**:
     ```bash
     uv run --env-file .env raw_data_to_sql_and_csv.py
     ```

## When to use what

- **SedonaDB** — data fits in memory, you want SQL semantics with Sedona's spatial functions without a server; Very fast, simple to use.
- **Shapely / Geopandas** — data fits in memory, Python-only stack, geometries beyond points.
- **DuckDB** — data fits in memory, you want SQL semantics without a server, or you are already working with Parquet/ORC files.
- **Scikit-Learn** — data fits in memory, points only. Could be faster than Shapely when finding only one neighbour per point, but cannot break ties by postcode (or any secondary sort key), so results may differ from the other implementations in those edge cases. It is not as flexible as the other python solutions so it might be used in particular cases.
- **SQL (PostgreSQL + PostGIS)** — data does not fit in memory, or you need to join against other tables and write complex queries.
- **C# (.NET / NetTopologySuite)** — already in the .NET ecosystem; API mirrors the JVM JTS library. Similar speed than the JVM solutions and easier to run as it requires less settings.
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
| SedonaDB                       | 0 days 00:00:00.438286 |
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
