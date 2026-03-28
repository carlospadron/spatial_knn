import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
database = os.getenv("DB_NAME", "gis")
uprn_table = os.getenv("UPRN_TABLE", "os.open_uprn_white_horse")
codepoint_table = os.getenv("CODEPOINT_TABLE", "os.code_point_open_white_horse")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

uprn = gpd.read_postgis(
    f"SELECT uprn, geom FROM {uprn_table}", engine, geom_col="geom"
)
codepoint = gpd.read_postgis(
    f"SELECT postcode, geom FROM {codepoint_table}", engine, geom_col="geom"
)

t1 = pd.Timestamp.now()

knn = (
    uprn.sjoin_nearest(codepoint, max_distance=5000, distance_col="distance")
    .rename(columns={"uprn": "origin", "postcode": "destination"})
    .sort_values(["origin", "destination"])
    .drop_duplicates(subset="origin")[["origin", "destination", "distance"]]
)

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
