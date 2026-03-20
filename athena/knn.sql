WITH knn AS (
    SELECT 
        A.UPRN as origin,
        B.postcode as destination,
        round(ST_Distance(ST_GeometryFromText(A.wkt), ST_GeometryFromText(B.wkt)), 2) as distance,
        row_number() OVER (
            PARTITION BY A.UPRN
            ORDER BY
                A.UPRN,
                ST_Distance(ST_GeometryFromText(A.wkt), ST_GeometryFromText(B.wkt)) ASC,
                B.postcode
        ) AS rn
    FROM
        open_uprn_white_horse as A,
        code_point_open_white_horse as B
    WHERE
        ST_Distance(ST_GeometryFromText(A.wkt), ST_GeometryFromText(B.wkt)) <= 5000
    ORDER BY
        A.UPRN,
        ST_Distance(ST_GeometryFromText(A.wkt), ST_GeometryFromText(B.wkt)) ASC,
        destination
)
SELECT origin, destination, distance FROM knn WHERE rn = 1;
