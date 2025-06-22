-- Compare outputs of knn_cartesian_join and knn_h3_join

-- Find rows in cartesian but not in H3
SELECT *
FROM 
    knn_cartesian_join,
    knn_h3_join
WHERE
    knn_cartesian_join.origin = knn_h3_join.origin
    AND knn_cartesian_join.destination != knn_h3_join.destination;