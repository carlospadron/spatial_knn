import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST', 'localhost')
port = os.getenv('DB_PORT', '5432')
database = os.getenv('DB_NAME', 'gis')
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

uprn = gpd.read_postgis("SELECT uprn, geom FROM os.open_uprn_white_horse", engine, geom_col='geom')
codepoint = gpd.read_postgis("SELECT postcode, geom FROM os.code_point_open_white_horse", engine, geom_col='geom')


def nearest_neighbour(geoma, geomb, maxdist):
    combinations = [
        geomb[geomb.intersects(i.buffer(maxdist))].distance(i).idxmin()
        for i in geoma
    ]
    return pd.DataFrame({
        'origin': [i[0] for i in geoma.items()],
        'destination': combinations,
        'distance': [i.distance(geomb[combinations[n]]) for n, i in enumerate(geoma)],
    })


t1 = pd.Timestamp.now()

knn = nearest_neighbour(uprn.geometry, codepoint.sort_values('postcode').geometry, 5000)
knn = uprn[['uprn']].assign(
    destination=codepoint.iloc[knn['destination']]['postcode'].to_list(),
    distance=knn.distance.to_list(),
)
knn.columns = ['origin', 'destination', 'distance']

t2 = pd.Timestamp.now()

knn.to_csv(Path(__file__).parent / 'result.csv', index=False)
print(t2 - t1)
