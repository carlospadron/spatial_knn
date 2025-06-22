USE WAREHOUSE BASIC; --xsmall

CREATE OR REPLACE TABLE knn_cartesian_join AS
WITH knn AS (
    SELECT 
        A.UPRN as origin,
        B."postcode" as destination,
        round(ST_Distance(A.geom_wkb, B.geom_wkb),2) as distance,
        row_number() OVER (PARTITION BY A.UPRN ORDER BY A.UPRN,ST_Distance(A.geom_wkb, B.geom_wkb) ASC, B."postcode") AS rn
    FROM
        GIS.PUBLIC.OPEN_UPRN_WHITE_HORSE as A,
        GIS.PUBLIC.CODE_POINT_OPEN_WHITE_HORSE as B
    WHERE
        ST_Distance(A.geom_wkb, B.geom_wkb) <= 5000
    ORDER BY
        A.UPRN,
        ST_Distance(A.geom_wkb, B.geom_wkb) ASC,
        destination
)
SELECT origin, destination, distance FROM knn WHERE rn = 1; 

--1m 14 s