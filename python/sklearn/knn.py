import argparse
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sklearn.neighbors import NearestNeighbors
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
    f"SELECT postcode, geom FROM {codepoint_table} ORDER BY postcode",
    engine,
    geom_col="geom",
)

t1 = pd.Timestamp.now()

uprn_xy = list(zip(uprn.geometry.x, uprn.geometry.y))
codep_xy = list(zip(codepoint.geometry.x, codepoint.geometry.y))

neigh = NearestNeighbors(n_neighbors=5, radius=5000)
neigh.fit(codep_xy)
distances, indices = neigh.kneighbors(uprn_xy)

knn = pd.DataFrame(
    {
        "origin": uprn["uprn"].repeat(5).to_list(),
        "destination": [codepoint.iloc[i]["postcode"] for row in indices for i in row],
        "distance": [d for row in distances for d in row],
    }
)
knn["distance"] = knn["distance"].round(2)
knn = knn.sort_values(["origin", "distance", "destination"]).drop_duplicates(
    subset="origin"
)

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
