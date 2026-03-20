import os

import duckdb
import pandas as pd

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST', 'localhost')
port = os.getenv('DB_PORT', '5432')
database = os.getenv('DB_NAME', 'gis')

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL postgres; LOAD postgres;")
con.execute(
    f"ATTACH 'host={host} port={port} dbname={database} user={user} password={password}' "
    "AS pg (TYPE postgres, READ_ONLY);"
)

# Load into local DuckDB tables (outside timing, consistent with other scripts)
con.execute("""
    CREATE TABLE uprn AS
    SELECT uprn::text AS uprn, geom FROM pg.os.open_uprn_white_horse
""")
con.execute("""
    CREATE TABLE codepoint AS
    SELECT postcode, geom FROM pg.os.code_point_open_white_horse
""")

# R-tree index enables fast KNN lookup without full scan
con.execute("CREATE INDEX codepoint_rtree ON codepoint USING RTREE (geom)")

t1 = pd.Timestamp.now()

con.execute("""
    COPY (
        SELECT
            u.uprn      AS origin,
            c.postcode  AS destination,
            ST_Distance(u.geom, c.geom) AS distance
        FROM uprn u
        CROSS JOIN LATERAL (
            SELECT postcode, geom
            FROM codepoint
            ORDER BY ST_Distance(u.geom, geom), postcode
            LIMIT 1
        ) c
    ) TO 'python/duckdb/result.csv' (HEADER, DELIMITER ',');
""")

t2 = pd.Timestamp.now()
print(t2 - t1)
