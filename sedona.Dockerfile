FROM apache/sedona:1.7.0
RUN pip install "sqlalchemy>=2" "geopandas>=1" psycopg2-binary --quiet --break-system-packages
