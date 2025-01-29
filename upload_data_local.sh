export PGPASSWORD=$DB_PASSWORD

psql -U $DB_USER -d gis -h localhost -f table_definitions.sql;
psql -U $DB_USER -d gis -h localhost -c "\COPY os.open_uprn_white_horse FROM 'data/open_uprn_white_horse.csv' DELIMITER '|' CSV ";  
psql -U $DB_USER -d gis -h localhost -c "\COPY os.code_point_open_white_horse FROM 'data/code_point_open_white_horse.csv' DELIMITER '|' CSV ";  
psql -U $DB_USER -d gis -h localhost -c "CREATE INDEX uprn_wh_gis ON os.open_uprn_white_horse USING gist(geom);"
psql -U $DB_USER -d gis -h localhost -c "CREATE INDEX cp_wh_gis ON os.code_point_open_white_horse USING gist(geom);"