import argparse
import os

import duckdb
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--uprn-table", default="os.open_uprn_white_horse")
parser.add_argument("--codepoint-table", default="os.code_point_open_white_horse")
args = parser.parse_args()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
database = os.getenv("DB_NAME", "gis")
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL postgres; LOAD postgres;")
con.execute(
    f"ATTACH 'host={host} port={port} dbname={database} user={user} password={password}' "
    "AS pg (TYPE postgres, READ_ONLY);"
)

# Load into local DuckDB tables (outside timing, consistent with other scripts)
con.execute(f"""
    CREATE TABLE uprn AS
    SELECT uprn::text AS uprn, geom FROM pg.{uprn_table}
""")
con.execute(f"""
    CREATE TABLE codepoint AS
    SELECT postcode, geom FROM pg.{codepoint_table}
""")

# R-tree index accelerates bounding-box joins (&&); DuckDB does NOT use it for
# ORDER BY ST_Distance LIMIT 1 (no KNN traversal), so we avoid LATERAL entirely.
con.execute("CREATE INDEX codepoint_rtree ON codepoint USING RTREE (geom)")

t1 = pd.Timestamp.now()

con.execute("""
    COPY (
        WITH candidates AS (
            SELECT
                u.uprn,
                c.postcode,
                ST_Distance(u.geom, c.geom) AS distance
            FROM uprn u
            JOIN codepoint c ON c.geom && ST_Expand(u.geom, 5000)
        ),
        ranked AS (
            SELECT
                uprn,
                postcode AS destination,
                distance,
                row_number() OVER (
                    PARTITION BY uprn
                    ORDER BY distance, postcode
                ) AS rn
            FROM candidates
        )
        SELECT uprn AS origin, destination, distance FROM ranked WHERE rn = 1
    ) TO 'python/duckdb/result.csv' (HEADER, DELIMITER ',');
""")

t2 = pd.Timestamp.now()
print(t2 - t1)
