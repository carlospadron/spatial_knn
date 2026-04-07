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

knn = sedona.sql("""
    WITH knn AS (
        SELECT
            A.uprn origin,
            B.postcode destination,
            ST_Distance(A.geom, B.geom) as distance,
            row_number() OVER (
                PARTITION BY A.uprn
                ORDER BY A.uprn, ST_Distance(A.geom, B.geom) ASC, B.postcode
            ) AS rn
        FROM
            uprn A
        JOIN
            codepoint B
        ON
            ST_KNN(A.geom, B.geom, 10, FALSE)
        ORDER BY
            A.uprn,
            ST_Distance(A.geom, B.geom) ASC,
            destination
    )
    SELECT origin, destination, distance FROM knn WHERE rn = 1
""").toPandas()

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / "result.csv", index=False)
print(t2 - t1)
