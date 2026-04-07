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


def nearest_neighbour(geoma, geomb, maxdist):
    results = []
    for idx, geom in geoma.items():
        nearby = geomb[geomb.intersects(geom.buffer(maxdist))].distance(geom)
        if nearby.empty:
            continue
        best = nearby.idxmin()
        results.append((idx, best, nearby[best]))
    return pd.DataFrame(results, columns=["origin_idx", "destination", "distance"])


t1 = pd.Timestamp.now()

knn = nearest_neighbour(uprn.geometry, codepoint.sort_values("postcode").geometry, 5000)
knn = pd.DataFrame({
    "origin": uprn.iloc[knn["origin_idx"]]["uprn"].to_list(),
    "destination": codepoint.iloc[knn["destination"]]["postcode"].to_list(),
    "distance": knn["distance"].to_list(),
})

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
