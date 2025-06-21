# %% [markdown]
# PySpark Spatial Data Preparation (No Database)

# %%
# Initialize Spark with Sedona
from sedona.spark import *
from pyspark.sql.functions import col, expr, concat_ws, collect_list

config = (
    SedonaContext
    .builder()
    .master("local[*]")
    .config('spark.jars.packages',
            'org.apache.sedona:sedona-spark-3.3_2.12:1.7.0,'
            'org.datasyslab:geotools-wrapper:1.7.0-28.5')
    .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all')
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()
)
spark = SedonaContext.create(config)

# %%
# Read OS UPRN CSV
uprn = spark.read.option("header", True).csv("data/osopenuprn_202506.csv")
uprn = uprn.withColumn("X_COORDINATE", col("X_COORDINATE").cast("double")) \
           .withColumn("Y_COORDINATE", col("Y_COORDINATE").cast("double"))

# %%
# Read local authorities and codepoint from GeoPackage
local_auth = spark.read.format("geopackage").option("tableName", "district_borough_unitary").load("data/bdline_gb.gpkg")
codepoint = spark.read.format("geopackage").option("tableName", "codepoint").load("data/codepo_gb.gpkg")

# %%
# Create geometry for UPRN points
uprn = uprn.withColumn(
    "geom",
    expr("ST_Point(X_COORDINATE, Y_COORDINATE)")
)
uprn = uprn.withColumn("geom", expr("ST_SetSRID(geom, 27700)"))

# %%
# Filter local authorities for 'Vale of White Horse District'
white_horse = local_auth.filter(col("name") == "Vale of White Horse District")

# %%
# Register temp views for SQL
uprn.createOrReplaceTempView("uprn")
white_horse.createOrReplaceTempView("white_horse")

# %%
# Spatial join: UPRN within White Horse
uprn_wh = spark.sql("""
SELECT u.*
FROM uprn u, white_horse w
WHERE ST_Intersects(u.geom, w.geometry)
""")

# %%
# CodePoint in White Horse
codepoint.createOrReplaceTempView("codepoint")
cp_wh = spark.sql("""
SELECT c.*
FROM codepoint c, white_horse w
WHERE ST_Intersects(c.geometry, w.geometry)
""")
cp_wh = cp_wh.groupBy("geometry").agg(
    concat_ws(",", collect_list(col("postcode").cast("string"))).alias("postcode")
)
cp_wh = cp_wh.withColumnRenamed("geometry", "geom")

# %%
# Write output as ORC
uprn_wh.write.mode("overwrite").orc("data/open_uprn_white_horse.orc")
cp_wh.write.mode("overwrite").orc("data/code_point_open_white_horse.orc")

# %%
