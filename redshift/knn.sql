CREATE TABLE os.knn AS
WITH knn AS (
    SELECT 
        A.UPRN as origin,
        B.postcode as destination,
        round(ST_Distance(A.geom, B.geom), 2) as distance,
        row_number() OVER (
            PARTITION BY A.UPRN
            ORDER BY A.UPRN, ST_Distance(A.geom, B.geom) ASC, B.postcode
        ) AS rn
    FROM
        os.open_uprn_white_horse as A,
        os.code_point_open_white_horse as B
    WHERE
        ST_DWithin(A.geom, B.geom, 5000)
    ORDER BY
        A.UPRN,
        ST_Distance(A.geom, B.geom) ASC,
        destination
)
SELECT origin, destination, distance FROM knn WHERE rn = 1;
