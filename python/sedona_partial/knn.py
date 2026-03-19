import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sedona.spark import SedonaContext
from sqlalchemy import create_engine

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST', 'localhost')
port = os.getenv('DB_PORT', '5432')
database = os.getenv('DB_NAME', 'gis')
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

config = (
    SedonaContext.builder()
    .master("local[*]")
    .config(
        'spark.jars.packages',
        'org.postgresql:postgresql:42.7.5,'
        'org.apache.sedona:sedona-spark-3.5_2.12:1.7.0,'
        'org.datasyslab:geotools-wrapper:1.7.0-28.5',
    )
    .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all')
    .config('spark.executor.memory', '12g')
    .config('spark.driver.memory', '12g')
    .getOrCreate()
)
sedona = SedonaContext.create(config)

uprn = gpd.read_postgis("SELECT uprn, geom FROM os.open_uprn_white_horse", engine)
codepoint = gpd.read_postgis(
    "SELECT postcode, geom FROM os.code_point_open_white_horse ORDER BY postcode", engine
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
    .sort(['origin', 'distance', 'destination'])
    .dropDuplicates(['origin'])
    .toPandas()
)

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / 'result.csv', index=False)
print(t2 - t1)
