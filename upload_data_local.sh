psql -d gis -f table_definitions.sql;
psql -d gis -c "\COPY os.open_uprn_white_horse FROM 'data/open_uprn_white_horse.csv' DELIMITER '|' CSV HEADER";  
psql -d gis -c "\COPY os.code_point_open_white_horse FROM 'data/code_point_open_white_horse.csv' DELIMITER '|' CSV HEADER";  