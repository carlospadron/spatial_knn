# %% [markdown]
# # imports

# %%
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import os

# %%
user=os.getenv('DB_USER')
password=os.getenv('DB_PASSWORD')
engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/gis')

# %% [markdown]
# # data

# %%
uprn = pd.read_csv('data/osopenuprn_202506.csv', nrows=1000)
uprn.info()

# %%
conn = engine.raw_connection()
cursor = conn.cursor()
sql = """CREATE SCHEMA os;"""
cursor.execute(sql)
conn.commit()
conn.close()

# %%
uprn.head(0).to_sql('os_open_uprn', 
                con=engine, 
                schema='os', 
                if_exists='replace', 
                method='multi', 
                index=False)

# %%
cd = f"""
psql "postgresql://{user}:{password}@localhost:5432/gis" -c "\COPY os.os_open_uprn FROM 'data/osopenuprn_202412_csv/osopenuprn_202412.csv' DELIMITER ',' CSV HEADER";  
    """
print(os.popen(cd).read())

# %%
cd = f"""  
ogr2ogr -a_srs EPSG:27700 -overwrite -f "PostgreSQL" PG:"host=localhost schemas=os user={user} dbname=gis password={password}" data/codepo_gpkg_gb/Data/codepo_gb.gpkg -nln "codepoint_points";  
    """
print(os.popen(cd).read())

# %%
cd = f"""
ogr2ogr -a_srs EPSG:27700 -overwrite -f "PostgreSQL" PG:"host=localhost schemas=os user={user} dbname=gis password={password}" data/bdline_gpkg_gb/Data/bdline_gb.gpkg district_borough_unitary -nln "local_authorities";
    """
print(os.popen(cd).read())

# %%
conn = engine.raw_connection()
cursor = conn.cursor()

#execute sql
sql="""
    ALTER TABLE os.os_open_uprn ADD COLUMN geom geometry('POINT', 27700);
    
    UPDATE os.os_open_uprn SET geom = ST_SetSRID(ST_Point("X_COORDINATE","Y_COORDINATE"), 27700);
    
    CREATE INDEX uprn_gix ON os.os_open_uprn USING GIST(geom);
    
    DROP TABLE IF EXISTS os.open_uprn_white_horse ;
    
    SELECT
        A.*
    INTO
        os.open_uprn_white_horse
    FROM
        os.os_open_uprn A,
        os.local_authorities B
    WHERE
        B.name = 'Vale of White Horse District'
        AND ST_intersects(A.geom, B.geometry);
    
    CREATE INDEX uprn_wh_gis ON os.open_uprn_white_horse USING gist(geom);
    
    DROP TABLE IF EXISTS os.code_point_open_white_horse;
    
    SELECT
        string_agg(A.postcode::text, ',') postcode,
        A.geometry
    INTO
        os.code_point_open_white_horse
    FROM
        os.codepoint_points A,
        os.local_authorities B
    WHERE
        B.name = 'Vale of White Horse District'
        AND ST_intersects(A.geometry, B.geometry)
    GROUP BY
        A.geometry;
    
    CREATE INDEX cp_wh_gis ON os.code_point_open_white_horse USING gist(geometry);
    
    ALTER TABLE os.code_point_open_white_horse RENAME COLUMN geometry TO geom;
    """

cursor.execute(sql)
conn.commit()
conn.close()

# %% [markdown]
# # Data export for cloud services

# %%
sql = """SELECT *, ST_AsText(geom) wkt FROM os.code_point_open_white_horse;"""
data = pd.read_sql(sql, engine).to_csv('data/code_point_open_white_horse.csv', index=False, header=False, sep='|')

sql = """SELECT *, ST_AsText(geom) wkt FROM os.open_uprn_white_horse;"""
data = pd.read_sql(sql, engine).to_csv('data/open_uprn_white_horse.csv', index=False, header=False, sep='|')


