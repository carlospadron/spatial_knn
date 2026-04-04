import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()
parser.add_argument("--uprn-table", default="os.open_uprn_white_horse")
parser.add_argument("--codepoint-table", default="os.code_point_open_white_horse")
parser.add_argument(
    "--statement-timeout",
    type=int,
    default=0,
    help="PostgreSQL statement_timeout in milliseconds (0 = no limit)",
)
args = parser.parse_args()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
database = os.getenv("DB_NAME", "gis")
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

t1 = pd.Timestamp.now()

conn = engine.raw_connection()
cursor = conn.cursor()
if args.statement_timeout:
    cursor.execute("SET statement_timeout = %s", (args.statement_timeout,))
cursor.execute(f"""
    DROP TABLE IF EXISTS os.knn_l;
    WITH knn AS (
        SELECT
            A.uprn as origin,
            B.postcode as destination,
            round(ST_Distance(A.geom, B.geom)::numeric, 2) as distance
        FROM
            {uprn_table} as A
        CROSS JOIN LATERAL (
            SELECT
                B.postcode,
                B.geom
            FROM
                {codepoint_table} as B
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
