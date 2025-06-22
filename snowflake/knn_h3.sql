USE WAREHOUSE BASIC; --xsmall

CREATE OR REPLACE TABLE knn_h3_join AS
WITH
-- Assign H3 index to each point
uprn_h3 AS (
    SELECT
        UPRN,
        geom_wkb,
        -- Use a suitable resolution, e.g., 9
        H3_COVERAGE(TO_GEOGRAPHY(ST_TRANSFORM(ST_Buffer(geom_wkb, 5000), 4326)), 7) AS h3
    FROM GIS.PUBLIC.OPEN_UPRN_WHITE_HORSE
)
select * from uprn_h3;

,
cp_h3 AS (
    SELECT
        "postcode" AS POSTCODE,
        geom_wkb,
        H3_COVERAGE(TO_GEOGRAPHY(ST_TRANSFORM(ST_Buffer(geom_wkb, 5000), 4326)), 9) AS h3
    FROM GIS.PUBLIC.CODE_POINT_OPEN_WHITE_HORSE
),
-- Join UPRNs to CodePoints in the same or neighboring H3 cells
candidate_pairs AS (
    SELECT
        u.UPRN AS origin,
        c.postcode AS destination,
        u.geom_wkb AS origin_geom,
        c.geom_wkb AS dest_geom
    FROM uprn_h3 u
    JOIN cp_h3 c
      ON ARRAYS_OVERLAP(u.h3, c.h3)
),
-- Calculate distances and rank
ranked AS (
    SELECT
        origin,
        destination,
        ROUND(ST_DISTANCE(origin_geom, dest_geom), 2) AS distance,
        ROW_NUMBER() OVER (
            PARTITION BY origin
            ORDER BY ST_DISTANCE(origin_geom, dest_geom) ASC, destination
        ) AS rn
    FROM candidate_pairs
)
SELECT origin, destination, distance
FROM ranked
WHERE rn = 1;
