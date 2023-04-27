# spatial_knn
A compilation of solutions for the KNN problem.

See notebook for speed comparison.

When to use what:
- Geopandas: 
	- the data fits the memory
- SQL
	- the data does not fit the memory
	- you plan to use many other tables already present in the database
	- you plan to write very complex queries
- Scala/Kotlin
	- the data fits the memory
	- you need to write a complex program of which KNN is just a part and you need it to run fast
- Rust 
	- the data fits the memory
	- you need to solve your KNN problem very very fast
- Pyspark
	- your data is massive and needs to be spread on different clusters
