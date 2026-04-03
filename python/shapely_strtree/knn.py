import argparse
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.strtree import STRtree
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

uprn = gpd.read_postgis(
    f"SELECT uprn, geom FROM {uprn_table}", engine, geom_col="geom"
)
codepoint = gpd.read_postgis(
    f"SELECT postcode, geom FROM {codepoint_table}", engine, geom_col="geom"
)


def nearest_neighbour(geoma, geomb):
    geomb_list = geomb.to_list()
    tree = STRtree(geomb_list)
    nearest = [
        [i[0], tree.query_nearest(i[1], return_distance=True)] for i in geoma.items()
    ]
    nearest = [[i[0], j, i[1][1][n]] for i in nearest for n, j in enumerate(i[1][0])]
    return pd.DataFrame(nearest, columns=["origin", "destination", "distance"])


t1 = pd.Timestamp.now()

knn = nearest_neighbour(uprn.geometry, codepoint.geometry)
knn = pd.merge(uprn, knn, left_index=True, right_on="origin")
knn = pd.merge(codepoint, knn, left_index=True, right_on="destination")
knn = (
    knn[["uprn", "postcode", "distance"]]
    .rename(columns={"uprn": "origin", "postcode": "destination"})
    .sort_values(["origin", "destination"])
    .drop_duplicates(subset="origin")
)

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
