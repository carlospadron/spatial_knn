import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sedona.db import SedonaDB
from sqlalchemy import create_engine

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
database = os.getenv("DB_NAME", "gis")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

sd = SedonaDB.connect()

# Load data outside the timed section (consistent with other scripts)
uprn = gpd.read_postgis(
    "SELECT uprn::text AS uprn, geom FROM os.open_uprn_white_horse",
    engine,
    geom_col="geom",
)
codepoint = gpd.read_postgis(
    "SELECT postcode, geom FROM os.code_point_open_white_horse ORDER BY postcode",
    engine,
    geom_col="geom",
)

sd.create_data_frame(uprn).to_view("uprn")
sd.create_data_frame(codepoint).to_view("codepoint")

t1 = pd.Timestamp.now()

knn = sd.sql("""
    WITH knn AS (
        SELECT
            u.uprn      AS origin,
            c.postcode  AS destination,
            ST_Distance(u.geom, c.geom) AS distance,
            row_number() OVER (
                PARTITION BY u.uprn
                ORDER BY ST_Distance(u.geom, c.geom) ASC, c.postcode
            ) AS rn
        FROM uprn u
        JOIN codepoint c ON ST_KNN(u.geom, c.geom, 10, FALSE)
    )
    SELECT origin, destination, distance FROM knn WHERE rn = 1
""").to_pandas()

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
