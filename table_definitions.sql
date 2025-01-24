DROP TABLE IF EXISTS os.open_uprn_white_horse;
CREATE TABLE os.open_uprn_white_horse (
    uprn bigint PRIMARY KEY,
    easting double precision,
    northing double precision,
    lat double precision,
    lon double precision,
    geom geometry(Point, 27700),
    wkt text
);

DROP TABLE IF EXISTS os.code_point_open_white_horse;
CREATE TABLE os.code_point_open_white_horse (
    postcode text PRIMARY KEY,
    geom geometry(Point, 27700),
    wkt text
);