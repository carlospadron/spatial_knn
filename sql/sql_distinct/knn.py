import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from knn_common import get_engine, get_parser

parser = get_parser()
parser.add_argument(
    "--statement-timeout",
    type=int,
    default=0,
    help="PostgreSQL statement_timeout in milliseconds (0 = no limit)",
)
args = parser.parse_args()
uprn_table = args.uprn_table
codepoint_table = args.codepoint_table
engine = get_engine()

t1 = pd.Timestamp.now()

conn = engine.raw_connection()
cursor = conn.cursor()
if args.statement_timeout:
    cursor.execute("SET statement_timeout = %s", (args.statement_timeout,))
cursor.execute(f"""
    CREATE INDEX IF NOT EXISTS idx_{codepoint_table.split('.')[-1]}_geom
    ON {codepoint_table} USING gist (geom);
""")
conn.commit()
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
