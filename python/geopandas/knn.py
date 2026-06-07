import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import geopandas as gpd
import pandas as pd

from knn_common import get_engine, get_parser

args = get_parser().parse_args()
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table
engine = get_engine()

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
