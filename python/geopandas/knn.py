import argparse
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

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
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

uprn = gpd.read_postgis(f"SELECT uprn, geom FROM {uprn_table}", engine, geom_col="geom")
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
