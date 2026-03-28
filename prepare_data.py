# %% [markdown]
# # Prepare spatial data — single entry point
#
# Two modes:
#   --from-raw   (default) Read raw CSV/GPKG → Spark/Sedona spatial join →
#                write ORC + CSV → load all 4 tables into PostGIS
#   --from-orc   Read existing ORC files → load all 4 tables into PostGIS
#                (no Spark required)
#
# Usage:
#   uv run --env-file .env prepare_data.py              # from-raw
#   uv run --env-file .env prepare_data.py --from-orc   # from-orc

# %%
import argparse
import io
import os

import pandas as pd
import pyarrow.dataset as pds
from sqlalchemy import create_engine, text

# %%
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
database = os.getenv("DB_NAME", "gis")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

ORC_PATHS = {
    "uprn_full": "data/os_open_uprn.orc",
    "cp_full": "data/codepoint_polygons.orc",
    "uprn_wh": "data/open_uprn_white_horse.orc",
    "cp_wh": "data/code_point_open_white_horse.orc",
}


# ---------------------------------------------------------------------------
# PostGIS helpers
# ---------------------------------------------------------------------------
def create_tables():
    with open("table_definitions.sql") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("Created schema and tables.")


def load_uprn_orc(orc_path, target_table, index_name):
    df = pds.dataset(orc_path, format="orc").to_table().to_pandas()
    df = df.rename(
        columns={
            "UPRN": "uprn",
            "X_COORDINATE": "easting",
            "Y_COORDINATE": "northing",
            "LATITUDE": "lat",
            "LONGITUDE": "lon",
        }
    )
    df["geom_hex"] = df["geom_wkb"].apply(
        lambda b: b.hex() if b is not None else None
    )
    stage = df[["uprn", "easting", "northing", "lat", "lon", "geom_hex"]].copy()

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(
            "CREATE TEMP TABLE _uprn_stage "
            "(uprn bigint, easting double precision, northing double precision, "
            "lat double precision, lon double precision, geom_hex text);"
        )
        buf = io.StringIO()
        stage.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_expert("COPY _uprn_stage FROM STDIN WITH (FORMAT text)", buf)
        cur.execute(f"""
            INSERT INTO {target_table} (uprn, easting, northing, lat, lon, geom, wkt)
            SELECT uprn, easting, northing, lat, lon,
                   ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700),
                   ST_AsText(ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700))
            FROM _uprn_stage
            ON CONFLICT (uprn) DO NOTHING;
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {target_table} USING gist(geom);")
        cur.execute("DROP TABLE _uprn_stage;")
        raw.commit()
    finally:
        raw.close()
    print(f"Loaded {target_table}: {len(stage)} rows")


def load_codepoint_orc(orc_path, target_table, index_name):
    df = pds.dataset(orc_path, format="orc").to_table().to_pandas()
    df["geom_hex"] = df["geom_wkb"].apply(
        lambda b: b.hex() if b is not None else None
    )
    stage = df[["postcode", "geom_hex"]].copy()

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("CREATE TEMP TABLE _cp_stage (postcode text, geom_hex text);")
        buf = io.StringIO()
        stage.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_expert("COPY _cp_stage FROM STDIN WITH (FORMAT text)", buf)
        cur.execute(f"""
            INSERT INTO {target_table} (postcode, geom, wkt)
            SELECT postcode,
                   ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700),
                   ST_AsText(ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700))
            FROM _cp_stage
            ON CONFLICT (postcode) DO NOTHING;
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {target_table} USING gist(geom);")
        cur.execute("DROP TABLE _cp_stage;")
        raw.commit()
    finally:
        raw.close()
    print(f"Loaded {target_table}: {len(stage)} rows")


def load_all_orc():
    create_tables()
    load_uprn_orc(ORC_PATHS["uprn_full"], "os.os_open_uprn", "uprn_full_gis")
    load_codepoint_orc(ORC_PATHS["cp_full"], "os.codepoint_polygons", "cp_full_gis")
    load_uprn_orc(ORC_PATHS["uprn_wh"], "os.open_uprn_white_horse", "uprn_wh_gis")
    load_codepoint_orc(ORC_PATHS["cp_wh"], "os.code_point_open_white_horse", "cp_wh_gis")
    print("Done — all tables loaded from ORC.")


# ---------------------------------------------------------------------------
# From raw: Spark/Sedona transform → ORC + CSV → PostGIS
# ---------------------------------------------------------------------------
def from_raw():
    from sedona.spark import SedonaContext
    from pyspark.sql.functions import col, expr, concat_ws, collect_list

    config = (
        SedonaContext.builder()
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-3.5_2.12:1.7.2,"
            "org.datasyslab:geotools-wrapper:1.7.2-28.5",
        )
        .config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all",
        )
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )
    spark = SedonaContext.create(config)

    # ---- Read raw sources ----
    # Glob picks up any release date variant, e.g. osopenuprn_202506.csv
    uprn = spark.read.option("header", True).csv("data/raw/osopenuprn_*.csv")
    uprn = uprn.withColumn("X_COORDINATE", col("X_COORDINATE").cast("double")).withColumn(
        "Y_COORDINATE", col("Y_COORDINATE").cast("double")
    )

    local_auth = (
        spark.read.format("geopackage")
        .option("tableName", "district_borough_unitary")
        .load("data/raw/bdline_gb.gpkg")
    )
    codepoint = (
        spark.read.format("geopackage")
        .option("tableName", "codepoint")
        .load("data/raw/codepo_gb.gpkg")
    )

    # ---- Geometry columns ----
    uprn = uprn.withColumn("geom", expr("ST_SetSRID(ST_Point(X_COORDINATE, Y_COORDINATE), 27700)"))

    # ---- Full GB datasets ----
    uprn_full = uprn.withColumn("geom_wkb", expr("ST_AsBinary(geom)"))
    uprn_full.write.mode("overwrite").orc(ORC_PATHS["uprn_full"])
    print(f"Wrote {ORC_PATHS['uprn_full']}")

    codepoint_with_geom = codepoint.withColumn("geom", expr("ST_SetSRID(geometry, 27700)"))
    cp_full = (
        codepoint_with_geom.groupBy("geometry")
        .agg(concat_ws(",", collect_list(col("postcode").cast("string"))).alias("postcode"))
        .withColumnRenamed("geometry", "geom")
        .withColumn("geom_wkb", expr("ST_AsBinary(geom)"))
    )
    cp_full.write.mode("overwrite").orc(ORC_PATHS["cp_full"])
    print(f"Wrote {ORC_PATHS['cp_full']}")

    # ---- White Horse filtered datasets ----
    white_horse = local_auth.filter(col("name") == "Vale of White Horse District")
    uprn.createOrReplaceTempView("uprn")
    white_horse.createOrReplaceTempView("white_horse")
    codepoint.createOrReplaceTempView("codepoint")

    uprn_wh = spark.sql("""
        SELECT u.* FROM uprn u, white_horse w
        WHERE ST_Intersects(u.geom, w.geometry)
    """)
    uprn_wh = uprn_wh.withColumn("geom_wkb", expr("ST_AsBinary(geom)"))
    uprn_wh.write.mode("overwrite").orc(ORC_PATHS["uprn_wh"])
    print(f"Wrote {ORC_PATHS['uprn_wh']}")

    cp_wh = spark.sql("""
        SELECT c.* FROM codepoint c, white_horse w
        WHERE ST_Intersects(c.geometry, w.geometry)
    """)
    cp_wh = (
        cp_wh.groupBy("geometry")
        .agg(concat_ws(",", collect_list(col("postcode").cast("string"))).alias("postcode"))
        .withColumnRenamed("geometry", "geom")
        .withColumn("geom_wkb", expr("ST_AsBinary(geom)"))
    )
    cp_wh.write.mode("overwrite").orc(ORC_PATHS["cp_wh"])
    print(f"Wrote {ORC_PATHS['cp_wh']}")

    # ---- CSV exports for cloud services ----
    uprn_wh_pd = uprn_wh.toPandas()
    uprn_wh_pd["wkt"] = uprn_wh_pd["geom_wkb"].apply(
        lambda b: None  # WKT generated from PostGIS on load
    )
    cp_wh_pd = cp_wh.toPandas()

    # Export via PostGIS (load first, then query back with WKT)
    spark.stop()
    print("Spark stopped. Loading into PostGIS…")

    load_all_orc()

    # CSV export from PostGIS (includes WKT)
    pd.read_sql("SELECT *, ST_AsText(geom) wkt FROM os.open_uprn_white_horse", engine).to_csv(
        "data/open_uprn_white_horse.csv", index=False, header=False, sep="|"
    )
    pd.read_sql("SELECT *, ST_AsText(geom) wkt FROM os.code_point_open_white_horse", engine).to_csv(
        "data/code_point_open_white_horse.csv", index=False, header=False, sep="|"
    )
    print("Wrote CSV exports for cloud services.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare spatial KNN data")
    parser.add_argument(
        "--from-orc",
        action="store_true",
        help="Load from existing ORC files (no Spark needed)",
    )
    args = parser.parse_args()

    if args.from_orc:
        load_all_orc()
    else:
        from_raw()
