import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
database = os.getenv("DB_NAME", "gis")
uprn_table = os.getenv("UPRN_TABLE", "os.open_uprn_white_horse")
codepoint_table = os.getenv("CODEPOINT_TABLE", "os.code_point_open_white_horse")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

t1 = pd.Timestamp.now()

conn = engine.raw_connection()
cursor = conn.cursor()
cursor.execute(f"""
    DROP TABLE IF EXISTS os.knn;
    WITH knn AS (
        SELECT DISTINCT ON (A.uprn)
            A.uprn as origin,
            B.postcode as destination,
            round(ST_Distance(A.geom, B.geom)::numeric, 2) as distance
        FROM
            {uprn_table} as A,
            {codepoint_table} as B
        WHERE
            ST_DWithin(A.geom, B.geom, 5000)
        ORDER BY
            A.uprn,
            ST_Distance(A.geom, B.geom) ASC,
            destination
    )
    SELECT * INTO os.knn FROM knn
""")
conn.commit()
conn.close()

t2 = pd.Timestamp.now()

result = pd.read_sql(
    "SELECT origin, destination, distance FROM os.knn ORDER BY origin", engine
)
result.to_csv(Path(__file__).parent / "result.csv", index=False)

print(t2 - t1)
