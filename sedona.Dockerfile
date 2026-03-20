FROM apache/sedona:latest
RUN pip install "sqlalchemy>=2" "geopandas>=1" psycopg2-binary --quiet --break-system-packages
