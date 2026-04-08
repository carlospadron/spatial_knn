# %% [markdown]
# # Prepare spatial data — single entry point
#
# Two modes:
#   --from-raw      (default) Read raw CSV/GPKG → DuckDB spatial join →
#                   write Parquet (zstd) + CSV → load all 4 tables into PostGIS
#   --from-parquet  Read existing Parquet files → load all 4 tables into PostGIS
#                   (no Spark required)
#
# Usage:
#   uv run --env-file .env prepare_data.py                 # from-raw
#   uv run --env-file .env prepare_data.py --from-parquet  # from-parquet

# %%
import argparse
import glob
import logging

import duckdb
import pandas as pd
from sqlalchemy import text

from knn_common import get_db_params, get_engine

# %%
db = get_db_params()
engine = get_engine()

PARQUET_PATHS = {
    "uprn_full": "data/os_open_uprn.parquet",
    "cp_full": "data/codepoint_polygons.parquet",
    "uprn_wh": "data/open_uprn_white_horse.parquet",
    "cp_wh": "data/code_point_open_white_horse.parquet",
}


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
_log = logging.getLogger(__name__).info


def _pg_attach(con):
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(
        "ATTACH 'host={host} port={port} dbname={database} user={user} password={password}' "
        "AS pg (TYPE POSTGRES)".format(**db)
    )


# ---------------------------------------------------------------------------
# PostGIS helpers
# ---------------------------------------------------------------------------
def create_tables():
    _log("Creating schema and tables from table_definitions.sql…")
    with open("table_definitions.sql") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    _log("  Done.")


def load_uprn_parquet(parquet_path, target_table, index_name, duck_con=None):
    _log(f"Loading {parquet_path} \u2192 {target_table}\u2026")
    con = duck_con or duckdb.connect()
    if not duck_con:
        _pg_attach(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE pg.public._uprn_stage AS
        SELECT UPRN AS uprn,
               X_COORDINATE AS easting,
               Y_COORDINATE AS northing,
               LATITUDE AS lat,
               LONGITUDE AS lon,
               geom_wkb
        FROM read_parquet('{parquet_path}')
    """)
    with engine.begin() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM _uprn_stage")).scalar()
        _log(f"  {n:,} rows staged \u2014 building geometry\u2026")
        conn.execute(
            text(f"""
            WITH g AS (
                SELECT uprn, easting, northing, lat, lon,
                       ST_SetSRID(ST_GeomFromWKB(geom_wkb), 27700) AS geom
                FROM _uprn_stage
            )
            INSERT INTO {target_table} (uprn, easting, northing, lat, lon, geom, wkt)
            SELECT uprn, easting, northing, lat, lon, geom, ST_AsText(geom)
            FROM g
            ON CONFLICT (uprn) DO NOTHING
        """)
        )
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {target_table} USING gist(geom)"
            )
        )
        conn.execute(text("DROP TABLE _uprn_stage"))
    _log(f"  {target_table} loaded ({n:,} rows).")


def load_codepoint_parquet(parquet_path, target_table, index_name, duck_con=None):
    _log(f"Loading {parquet_path} \u2192 {target_table}\u2026")
    con = duck_con or duckdb.connect()
    if not duck_con:
        _pg_attach(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE pg.public._cp_stage AS
        SELECT postcode, geom_wkb
        FROM read_parquet('{parquet_path}')
    """)
    with engine.begin() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM _cp_stage")).scalar()
        _log(f"  {n:,} rows staged \u2014 building geometry\u2026")
        conn.execute(
            text(f"""
            WITH g AS (
                SELECT postcode,
                       ST_SetSRID(ST_GeomFromWKB(geom_wkb), 27700) AS geom
                FROM _cp_stage
            )
            INSERT INTO {target_table} (postcode, geom, wkt)
            SELECT postcode, geom, ST_AsText(geom)
            FROM g
            ON CONFLICT (postcode) DO NOTHING
        """)
        )
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {target_table} USING gist(geom)"
            )
        )
        conn.execute(text("DROP TABLE _cp_stage"))
    _log(f"  {target_table} loaded ({n:,} rows).")


def load_all_parquet():
    create_tables()
    con = duckdb.connect()
    _pg_attach(con)
    load_uprn_parquet(PARQUET_PATHS["uprn_full"], "os.os_open_uprn", "uprn_full_gis", duck_con=con)
    load_codepoint_parquet(
        PARQUET_PATHS["cp_full"], "os.codepoint_polygons", "cp_full_gis", duck_con=con,
    )
    load_uprn_parquet(
        PARQUET_PATHS["uprn_wh"], "os.open_uprn_white_horse", "uprn_wh_gis", duck_con=con,
    )
    load_codepoint_parquet(
        PARQUET_PATHS["cp_wh"], "os.code_point_open_white_horse", "cp_wh_gis", duck_con=con,
    )
    con.close()
    _log("All tables loaded from Parquet.")


# ---------------------------------------------------------------------------
# From raw: DuckDB spatial join → Parquet + CSV → PostGIS
# ---------------------------------------------------------------------------
def from_raw():
    _log("Initialising DuckDB with spatial extension…")
    con = duckdb.connect()
    con.execute("LOAD spatial")

    csv_files = sorted(glob.glob("data/raw/osopenuprn_*.csv"))
    if not csv_files:
        raise FileNotFoundError("No osopenuprn_*.csv found under data/raw/")
    csv_path = csv_files[-1].replace("\\", "/")

    # ---- Load UPRN CSV into DuckDB (single read, reused for full + WH) ----
    _log(f"Reading UPRN CSV: {csv_path}")
    con.execute(f"""
        CREATE TABLE _uprn AS
        SELECT UPRN, X_COORDINATE, Y_COORDINATE, LATITUDE, LONGITUDE,
               ST_Point(X_COORDINATE, Y_COORDINATE) AS geom
        FROM read_csv('{csv_path}', types={{'UPRN': 'BIGINT'}})
    """)
    n_uprn = con.execute("SELECT COUNT(*) FROM _uprn").fetchone()[0]
    _log(f"  {n_uprn:,} UPRN records")

    # ---- Load CodePoint GeoPackage into DuckDB (single read, reused for full + WH) ----
    _log("Reading CodePoint GeoPackage…")
    con.execute("""
        CREATE TABLE _codepoint AS
        SELECT postcode, geometry AS geom
        FROM ST_Read('data/raw/codepo_gb.gpkg', layer='codepoint')
    """)
    n_cp = con.execute("SELECT COUNT(*) FROM _codepoint").fetchone()[0]
    _log(f"  {n_cp:,} codepoint records")

    # ---- Read White Horse boundary ----
    _log("Reading district boundaries GeoPackage…")
    con.execute("""
        CREATE TABLE _wh AS
        SELECT geometry AS geom
        FROM ST_Read('data/raw/bdline_gb.gpkg', layer='district_borough_unitary')
        WHERE Name = 'Vale of White Horse District'
    """)
    _log("  White Horse boundary ready")

    # ---- Write full GB Parquet files ----
    _log("Writing full GB UPRN Parquet…")
    con.execute(f"""
        COPY (SELECT UPRN, X_COORDINATE, Y_COORDINATE, LATITUDE, LONGITUDE, ST_AsWKB(geom) AS geom_wkb FROM _uprn)
        TO '{PARQUET_PATHS["uprn_full"]}' (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 19)
    """)
    _log(f"  Wrote {PARQUET_PATHS['uprn_full']} ({n_uprn:,} rows)")

    _log("Writing full GB CodePoint Parquet…")
    n_cp_full = con.execute("SELECT COUNT(*) FROM _codepoint").fetchone()[0]
    con.execute(f"""
        COPY (SELECT postcode, ST_AsWKB(geom) AS geom_wkb FROM _codepoint)
        TO '{PARQUET_PATHS["cp_full"]}' (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 19)
    """)
    _log(f"  Wrote {PARQUET_PATHS['cp_full']} ({n_cp_full:,} rows)")

    # ---- Spatial filter to White Horse ----
    _log("Filtering UPRN to White Horse via ST_Intersects…")
    n_uprn_wh = con.execute("""
        SELECT COUNT(*) FROM _uprn
        WHERE ST_Intersects(geom, (SELECT geom FROM _wh))
    """).fetchone()[0]
    _log(f"  {n_uprn_wh:,} UPRN within White Horse")
    con.execute(f"""
        COPY (SELECT UPRN, X_COORDINATE, Y_COORDINATE, LATITUDE, LONGITUDE, ST_AsWKB(geom) AS geom_wkb
              FROM _uprn WHERE ST_Intersects(geom, (SELECT geom FROM _wh)))
        TO '{PARQUET_PATHS["uprn_wh"]}' (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 19)
    """)
    _log(f"  Wrote {PARQUET_PATHS['uprn_wh']}")

    _log("Filtering CodePoint to White Horse via ST_Intersects…")
    n_cp_wh = con.execute("""
        SELECT COUNT(*) FROM _codepoint
        WHERE ST_Intersects(geom, (SELECT geom FROM _wh))
    """).fetchone()[0]
    _log(f"  {n_cp_wh:,} codepoint records within White Horse")
    con.execute(f"""
        COPY (SELECT postcode, ST_AsWKB(geom) AS geom_wkb
              FROM _codepoint WHERE ST_Intersects(geom, (SELECT geom FROM _wh)))
        TO '{PARQUET_PATHS["cp_wh"]}' (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 19)
    """)
    _log(f"  Wrote {PARQUET_PATHS['cp_wh']} ({n_cp_wh:,} rows)")

    # ---- CSV exports for cloud services (from DuckDB before closing) ----
    _log("Writing CSV exports for cloud services…")
    con.execute(f"""
        COPY (SELECT UPRN, X_COORDINATE, Y_COORDINATE, LATITUDE, LONGITUDE,
                     ST_AsText(geom) AS wkt
              FROM _uprn WHERE ST_Intersects(geom, (SELECT geom FROM _wh)))
        TO 'data/open_uprn_white_horse.csv' (HEADER FALSE, DELIMITER '|')
    """)
    con.execute(f"""
        COPY (SELECT postcode, ST_AsText(geom) AS wkt
              FROM _codepoint WHERE ST_Intersects(geom, (SELECT geom FROM _wh)))
        TO 'data/code_point_open_white_horse.csv' (HEADER FALSE, DELIMITER '|')
    """)
    _log("  Wrote CSV exports")

    con.close()

    # ---- Load into PostGIS ----
    _log("Loading all Parquet files into PostGIS…")
    load_all_parquet()
    _log("Done.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare spatial KNN data")
    parser.add_argument(
        "--from-parquet",
        action="store_true",
        help="Load from existing Parquet files (no Spark needed)",
    )
    args = parser.parse_args()

    if args.from_parquet:
        load_all_parquet()
    else:
        from_raw()
