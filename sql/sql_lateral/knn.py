import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
database = os.getenv("DB_NAME", "gis")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

t1 = pd.Timestamp.now()

conn = engine.raw_connection()
cursor = conn.cursor()
cursor.execute("""
    DROP TABLE IF EXISTS os.knn_l;
    WITH knn AS (
        SELECT
            A.uprn as origin,
            B.postcode as destination,
            round(ST_Distance(A.geom, B.geom)::numeric, 2) as distance
        FROM
            os.open_uprn_white_horse as A
        CROSS JOIN LATERAL (
            SELECT
                B.postcode,
                B.geom
            FROM
                os.code_point_open_white_horse as B
            WHERE
                ST_DWithin(A.geom, B.geom, 5000)
            ORDER BY
                ST_Distance(A.geom, B.geom) ASC,
                B.postcode
            LIMIT 1
        ) B
        ORDER BY
            A.uprn
    )
    SELECT * INTO os.knn_l FROM knn
""")
conn.commit()
conn.close()

t2 = pd.Timestamp.now()

result = pd.read_sql(
    "SELECT origin, destination, distance FROM os.knn_l ORDER BY origin", engine
)
result.to_csv(Path(__file__).parent / "result.csv", index=False)

print(t2 - t1)
