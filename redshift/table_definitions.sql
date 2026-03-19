CREATE SCHEMA os;

CREATE TABLE IF NOT EXISTS os.code_point_open_white_horse
(
    postcode text,
    geom geometry
);

CREATE TABLE IF NOT EXISTS os.open_uprn_white_horse
(
    uprn bigint,
    "X_COORDINATE" double precision,
    "Y_COORDINATE" double precision,
    "LATITUDE" double precision,
    "LONGITUDE" double precision,
    geom geometry
);
