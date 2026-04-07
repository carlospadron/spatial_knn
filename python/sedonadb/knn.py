import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import geopandas as gpd
import pandas as pd
from sedona.db import connect

from knn_common import get_engine, get_parser

args = get_parser().parse_args()
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table
engine = get_engine()

sd = connect()

# Load data outside the timed section (consistent with other scripts)
uprn = gpd.read_postgis(
    f"SELECT uprn::text AS uprn, geom FROM {uprn_table}",
    engine,
    geom_col="geom",
)
codepoint = gpd.read_postgis(
    f"SELECT postcode, geom FROM {codepoint_table} ORDER BY postcode",
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
            round(ST_Distance(u.geom, c.geom), 2) AS distance,
            row_number() OVER (
                PARTITION BY u.uprn
                ORDER BY round(ST_Distance(u.geom, c.geom), 2) ASC, c.postcode
            ) AS rn
        FROM uprn u
        JOIN codepoint c ON ST_KNN(u.geom, c.geom, 10, FALSE)
    )
    SELECT origin, destination, distance FROM knn WHERE rn = 1
""").to_pandas()

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
