import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import geopandas as gpd
import pandas as pd
from shapely.strtree import STRtree

from knn_common import get_engine, get_parser

args = get_parser().parse_args()
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table
engine = get_engine()

uprn = gpd.read_postgis(f"SELECT uprn, geom FROM {uprn_table}", engine, geom_col="geom")
codepoint = gpd.read_postgis(
    f"SELECT postcode, geom FROM {codepoint_table}", engine, geom_col="geom"
)


def nearest_neighbour(geoma, geomb):
    geomb_list = geomb.to_list()
    tree = STRtree(geomb_list)
    nearest = [
        [i[0], tree.query_nearest(i[1], max_distance=5000, return_distance=True)] for i in geoma.items()
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
