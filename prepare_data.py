# %% [markdown]
# # Prepare spatial data — single entry point
#
# Two modes:
#   --from-raw   (default) Read raw CSV/GPKG → SedonaDB spatial join →
#                write ORC + CSV → load all 4 tables into PostGIS
#   --from-orc   Read existing ORC files → load all 4 tables into PostGIS
#                (no Spark required)
#
# Usage:
#   uv run --env-file .env prepare_data.py              # from-raw
#   uv run --env-file .env prepare_data.py --from-orc   # from-orc

# %%
import argparse
import glob
import io
import os
import shutil

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as pa_orc
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


def _log(msg):
    print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _write_orc(df, path):
    """Write a pandas DataFrame as a single ORC file, replacing any existing file or directory."""
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pa_orc.write_table(pa.Table.from_pandas(df, preserve_index=False), path, compression="snappy")


def _aggregate_codepoint(gdf):
    """Group a codepoint GeoDataFrame by geometry, merging postcodes at the same centroid."""
    gdf = gdf.copy()
    gdf["_wkt"] = gdf.geometry.to_wkt()
    agg = gdf.groupby("_wkt", sort=False)["postcode"].agg(",".join).reset_index()
    agg["geom_wkb"] = gpd.GeoSeries.from_wkt(agg["_wkt"]).apply(lambda g: g.wkb)
    return agg[["postcode", "geom_wkb"]]


def _geom_to_wkb(value):
    """Normalise a geometry value (shapely, bytes, or bytearray) to raw WKB bytes."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if hasattr(value, "wkb"):
        return value.wkb
    return None


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


def load_uprn_orc(orc_path, target_table, index_name):
    _log(f"Loading {orc_path} → {target_table}…")
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
    _log(f"  {len(stage):,} rows read — copying to staging table…")

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
        _log("  COPY done — inserting into target table…")
        cur.execute(f"""
            INSERT INTO {target_table} (uprn, easting, northing, lat, lon, geom, wkt)
            SELECT uprn, easting, northing, lat, lon,
                   ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700),
                   ST_AsText(ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700))
            FROM _uprn_stage
            ON CONFLICT (uprn) DO NOTHING;
        """)
        _log(f"  Building spatial index {index_name}…")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {target_table} USING gist(geom);")
        cur.execute("DROP TABLE _uprn_stage;")
        raw.commit()
    finally:
        raw.close()
    _log(f"  {target_table} loaded ({len(stage):,} rows).")


def load_codepoint_orc(orc_path, target_table, index_name):
    _log(f"Loading {orc_path} → {target_table}…")
    df = pds.dataset(orc_path, format="orc").to_table().to_pandas()
    df["geom_hex"] = df["geom_wkb"].apply(
        lambda b: b.hex() if b is not None else None
    )
    stage = df[["postcode", "geom_hex"]].copy()
    _log(f"  {len(stage):,} rows read — copying to staging table…")

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("CREATE TEMP TABLE _cp_stage (postcode text, geom_hex text);")
        buf = io.StringIO()
        stage.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_expert("COPY _cp_stage FROM STDIN WITH (FORMAT text)", buf)
        _log("  COPY done — inserting into target table…")
        cur.execute(f"""
            INSERT INTO {target_table} (postcode, geom, wkt)
            SELECT postcode,
                   ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700),
                   ST_AsText(ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700))
            FROM _cp_stage
            ON CONFLICT (postcode) DO NOTHING;
        """)
        _log(f"  Building spatial index {index_name}…")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {target_table} USING gist(geom);")
        cur.execute("DROP TABLE _cp_stage;")
        raw.commit()
    finally:
        raw.close()
    _log(f"  {target_table} loaded ({len(stage):,} rows).")


def load_all_orc():
    create_tables()
    load_uprn_orc(ORC_PATHS["uprn_full"], "os.os_open_uprn", "uprn_full_gis")
    load_codepoint_orc(ORC_PATHS["cp_full"], "os.codepoint_polygons", "cp_full_gis")
    load_uprn_orc(ORC_PATHS["uprn_wh"], "os.open_uprn_white_horse", "uprn_wh_gis")
    load_codepoint_orc(ORC_PATHS["cp_wh"], "os.code_point_open_white_horse", "cp_wh_gis")
    _log("All tables loaded from ORC.")


# ---------------------------------------------------------------------------
# From raw: SedonaDB spatial join → ORC + CSV → PostGIS
# ---------------------------------------------------------------------------
def from_raw():
    from sedona.db import connect

    _log("Initialising SedonaDB (single-node)…")
    sd = connect()

    # ---- Read raw sources ----
    csv_files = sorted(glob.glob("data/raw/osopenuprn_*.csv"))
    if not csv_files:
        raise FileNotFoundError("No osopenuprn_*.csv found under data/raw/")
    _log(f"Reading UPRN CSV: {csv_files[-1]}")
    uprn_pd = pd.read_csv(csv_files[-1], dtype={"UPRN": "int64"})
    _log(f"  {len(uprn_pd):,} UPRN records")

    _log("Reading CodePoint GeoPackage…")
    codepoint_gdf = gpd.read_file("data/raw/codepo_gb.gpkg", layer="codepoint")
    codepoint_gdf = codepoint_gdf.set_crs("EPSG:27700", allow_override=True)
    _log(f"  {len(codepoint_gdf):,} codepoint records")

    _log("Reading district boundaries GeoPackage…")
    local_auth = gpd.read_file("data/raw/bdline_gb.gpkg", layer="district_borough_unitary")
    white_horse = (
        local_auth[local_auth["Name"] == "Vale of White Horse District"]
        .to_crs("EPSG:27700")
    )
    _log("  White Horse boundary ready")

    # ---- Build UPRN GeoDataFrame ----
    _log("Building UPRN GeoDataFrame…")
    uprn_gdf = gpd.GeoDataFrame(
        uprn_pd,
        geometry=gpd.points_from_xy(uprn_pd["X_COORDINATE"], uprn_pd["Y_COORDINATE"]),
        crs="EPSG:27700",
    )

    # ---- Write full GB ORC files (no spatial filter needed) ----
    _log("Writing full GB UPRN ORC…")
    uprn_full = uprn_pd.copy()
    uprn_full["geom_wkb"] = uprn_gdf.geometry.apply(lambda g: g.wkb)
    _write_orc(uprn_full, ORC_PATHS["uprn_full"])
    _log(f"  Wrote {ORC_PATHS['uprn_full']} ({len(uprn_full):,} rows)")

    _log("Writing full GB CodePoint ORC…")
    cp_full = _aggregate_codepoint(codepoint_gdf)
    _write_orc(cp_full, ORC_PATHS["cp_full"])
    _log(f"  Wrote {ORC_PATHS['cp_full']} ({len(cp_full):,} rows)")

    # ---- Register views in SedonaDB for spatial filtering ----
    _log("Registering SedonaDB views…")
    sd.create_data_frame(uprn_gdf).to_view("uprn")
    sd.create_data_frame(white_horse).to_view("white_horse")
    sd.create_data_frame(codepoint_gdf).to_view("codepoint")

    # ---- Spatial filter to White Horse ----
    _log("Filtering UPRN to White Horse via ST_Intersects…")
    uprn_wh_pd = sd.sql("""
        SELECT u.UPRN, u.X_COORDINATE, u.Y_COORDINATE, u.LATITUDE, u.LONGITUDE, u.geometry
        FROM uprn u, white_horse w
        WHERE ST_Intersects(u.geometry, w.geometry)
    """).to_pandas()
    _log(f"  {len(uprn_wh_pd):,} UPRN within White Horse")

    _log("Filtering CodePoint to White Horse via ST_Intersects…")
    cp_wh_raw = sd.sql("""
        SELECT c.postcode, c.geometry
        FROM codepoint c, white_horse w
        WHERE ST_Intersects(c.geometry, w.geometry)
    """).to_pandas()
    _log(f"  {len(cp_wh_raw):,} codepoint records within White Horse")

    # ---- Write White Horse ORC files ----
    _log("Writing White Horse UPRN ORC…")
    uprn_wh_pd["geom_wkb"] = uprn_wh_pd["geometry"].apply(_geom_to_wkb)
    uprn_wh_out = uprn_wh_pd.drop(columns=["geometry"])
    _write_orc(uprn_wh_out, ORC_PATHS["uprn_wh"])
    _log(f"  Wrote {ORC_PATHS['uprn_wh']}")

    _log("Writing White Horse CodePoint ORC…")
    cp_wh_pd = cp_wh_raw.copy()
    cp_wh_pd["geom_wkb"] = cp_wh_pd["geometry"].apply(_geom_to_wkb)
    cp_wh_agg = (
        cp_wh_pd.groupby("geom_wkb", sort=False)["postcode"]
        .agg(",".join)
        .reset_index()[["postcode", "geom_wkb"]]
    )
    _write_orc(cp_wh_agg, ORC_PATHS["cp_wh"])
    _log(f"  Wrote {ORC_PATHS['cp_wh']} ({len(cp_wh_agg):,} rows)")

    # ---- Load into PostGIS ----
    _log("Loading all ORC files into PostGIS…")
    load_all_orc()

    # ---- CSV exports for cloud services ----
    _log("Writing CSV exports for cloud services…")
    pd.read_sql("SELECT *, ST_AsText(geom) wkt FROM os.open_uprn_white_horse", engine).to_csv(
        "data/open_uprn_white_horse.csv", index=False, header=False, sep="|"
    )
    pd.read_sql("SELECT *, ST_AsText(geom) wkt FROM os.code_point_open_white_horse", engine).to_csv(
        "data/code_point_open_white_horse.csv", index=False, header=False, sep="|"
    )
    _log("Done.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare spatial KNN data")
    parser.add_argument(
        "--from-orc",
        action="store_true",
        help="Load from existing ORC files (no SedonaDB needed)",
    )
    args = parser.parse_args()

    if args.from_orc:
        load_all_orc()
    else:
        from_raw()
