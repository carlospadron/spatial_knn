# %% [markdown]
# # Load ORC files into local PostGIS database
# Reads the preprocessed ORC files produced by prepare_data_spark.py
# and inserts them into the PostGIS tables defined in table_definitions.sql.

# %%
import os
import pyarrow.dataset as ds
from sqlalchemy import create_engine, text

# %%
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/gis')

# %% [markdown]
# ## Create schema and tables

# %%
with open('table_definitions.sql') as f:
    ddl = f.read()

with engine.begin() as conn:
    conn.execute(text(ddl))

# %% [markdown]
# ## Load open_uprn_white_horse

# %%
uprn_table = ds.dataset('data/open_uprn_white_horse.orc', format='orc').to_table()
uprn_df = uprn_table.to_pandas()
print("UPRN ORC columns:", uprn_df.columns.tolist())
uprn_df.head(2)

# %%
# Rename raw CSV columns to match the PostGIS table schema
uprn_df = uprn_df.rename(columns={
    'UPRN': 'uprn',
    'X_COORDINATE': 'easting',
    'Y_COORDINATE': 'northing',
    'LATITUDE': 'lat',
    'LONGITUDE': 'lon',
})

# geom_wkb is a bytes column written by ST_AsBinary — insert via ST_GeomFromWKB
uprn_df['geom_hex'] = uprn_df['geom_wkb'].apply(lambda b: b.hex() if b is not None else None)

# %%
# Stage rows without geometry, then update geom from the hex WKB
stage_cols = ['uprn', 'easting', 'northing', 'lat', 'lon', 'geom_hex']
uprn_stage = uprn_df[stage_cols].copy()

with engine.begin() as conn:
    conn.execute(text("CREATE TEMP TABLE uprn_stage (uprn bigint, easting double precision, northing double precision, lat double precision, lon double precision, geom_hex text) ON COMMIT DROP;"))
    uprn_stage.to_sql('uprn_stage', conn, if_exists='append', index=False, method='multi')
    conn.execute(text("""
        INSERT INTO os.open_uprn_white_horse (uprn, easting, northing, lat, lon, geom, wkt)
        SELECT
            uprn,
            easting,
            northing,
            lat,
            lon,
            ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700),
            ST_AsText(ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700))
        FROM uprn_stage
        ON CONFLICT (uprn) DO NOTHING;
    """))

print("Loaded open_uprn_white_horse:", len(uprn_stage), "rows")

# %% [markdown]
# ## Load code_point_open_white_horse

# %%
cp_table = ds.dataset('data/code_point_open_white_horse.orc', format='orc').to_table()
cp_df = cp_table.to_pandas()
print("CodePoint ORC columns:", cp_df.columns.tolist())
cp_df.head(2)

# %%
cp_df['geom_hex'] = cp_df['geom_wkb'].apply(lambda b: b.hex() if b is not None else None)

cp_stage = cp_df[['postcode', 'geom_hex']].copy()

with engine.begin() as conn:
    conn.execute(text("CREATE TEMP TABLE cp_stage (postcode text, geom_hex text) ON COMMIT DROP;"))
    cp_stage.to_sql('cp_stage', conn, if_exists='append', index=False, method='multi')
    conn.execute(text("""
        INSERT INTO os.code_point_open_white_horse (postcode, geom, wkt)
        SELECT
            postcode,
            ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700),
            ST_AsText(ST_SetSRID(ST_GeomFromWKB(decode(geom_hex, 'hex')), 27700))
        FROM cp_stage
        ON CONFLICT (postcode) DO NOTHING;
    """))

print("Loaded code_point_open_white_horse:", len(cp_stage), "rows")

# %% [markdown]
# ## Create spatial indexes

# %%
with engine.begin() as conn:
    conn.execute(text("CREATE INDEX IF NOT EXISTS uprn_wh_gis ON os.open_uprn_white_horse USING gist(geom);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS cp_wh_gis ON os.code_point_open_white_horse USING gist(geom);"))

print("Done — spatial indexes created.")
