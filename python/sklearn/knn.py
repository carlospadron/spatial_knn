import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import geopandas as gpd
import pandas as pd
from sklearn.neighbors import NearestNeighbors

from knn_common import get_engine, get_parser

args = get_parser().parse_args()
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table
engine = get_engine()

uprn = gpd.read_postgis(f"SELECT uprn, geom FROM {uprn_table}", engine, geom_col="geom")
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
