import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import geopandas as gpd
import pandas as pd

from knn_common import get_engine, get_parser, get_sedona_context

args = get_parser().parse_args()
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table
engine = get_engine()
sedona = get_sedona_context()

uprn = gpd.read_postgis(f"SELECT uprn, geom FROM {uprn_table}", engine)
codepoint = gpd.read_postgis(
    f"SELECT postcode, geom FROM {codepoint_table} ORDER BY postcode",
    engine,
)

uprn_df = sedona.createDataFrame(uprn)
codepoint_df = sedona.createDataFrame(codepoint)
uprn_df.createOrReplaceTempView("uprn")
codepoint_df.createOrReplaceTempView("codepoint")

t1 = pd.Timestamp.now()

knn = (
    sedona.sql("""
        SELECT
            A.uprn origin,
            B.postcode destination,
            ST_Distance(A.geom, B.geom) as distance
        FROM
            uprn A,
            codepoint B
        WHERE
            ST_Distance(A.geom, B.geom) <= 5000
    """)
    .sort(["origin", "distance", "destination"])
    .dropDuplicates(["origin"])
    .toPandas()
)

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
