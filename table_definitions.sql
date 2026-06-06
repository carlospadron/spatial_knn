CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS os;
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

-- Full Great Britain dataset tables
DROP TABLE IF EXISTS os.os_open_uprn;
CREATE TABLE os.os_open_uprn (
    uprn bigint PRIMARY KEY,
    easting double precision,
    northing double precision,
    lat double precision,
    lon double precision,
    geom geometry(Point, 27700),
    wkt text
);

DROP TABLE IF EXISTS os.codepoint_polygons;
CREATE TABLE os.codepoint_polygons (
    postcode text PRIMARY KEY,
    geom geometry(Point, 27700),
    wkt text
);

-- White Horse district boundary (used to derive buffer scenarios)
DROP TABLE IF EXISTS os.white_horse_boundary;
CREATE TABLE os.white_horse_boundary (
    id serial PRIMARY KEY,
    geom geometry(Geometry, 27700)
);

-- 1 km buffer around White Horse district
DROP TABLE IF EXISTS os.uprn_wh_1km;
CREATE TABLE os.uprn_wh_1km (
    uprn bigint PRIMARY KEY,
    easting double precision,
    northing double precision,
    lat double precision,
    lon double precision,
    geom geometry(Point, 27700),
    wkt text
);
DROP TABLE IF EXISTS os.cp_wh_1km;
CREATE TABLE os.cp_wh_1km (
    postcode text PRIMARY KEY,
    geom geometry(Point, 27700),
    wkt text
);

-- 10 km buffer around White Horse district
DROP TABLE IF EXISTS os.uprn_wh_10km;
CREATE TABLE os.uprn_wh_10km (
    uprn bigint PRIMARY KEY,
    easting double precision,
    northing double precision,
    lat double precision,
    lon double precision,
    geom geometry(Point, 27700),
    wkt text
);
DROP TABLE IF EXISTS os.cp_wh_10km;
CREATE TABLE os.cp_wh_10km (
    postcode text PRIMARY KEY,
    geom geometry(Point, 27700),
    wkt text
);

-- 100 km buffer around White Horse district
DROP TABLE IF EXISTS os.uprn_wh_100km;
CREATE TABLE os.uprn_wh_100km (
    uprn bigint PRIMARY KEY,
    easting double precision,
    northing double precision,
    lat double precision,
    lon double precision,
    geom geometry(Point, 27700),
    wkt text
);
DROP TABLE IF EXISTS os.cp_wh_100km;
CREATE TABLE os.cp_wh_100km (
    postcode text PRIMARY KEY,
    geom geometry(Point, 27700),
    wkt text
);