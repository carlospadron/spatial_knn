# spatial_knn
A compilation of solutions for the KNN problem.

See notebook for speed comparison.

When to use what:
- Geopandas: 
	- the data fits the memory
	- you don't plan to do complex queries.
- SKlearn:
	- the previous conditions
	- your data is only points and Geopandas wasn't fast enough
- SQL
	- the data does not fit the memory
	- you plan to use many other tables already present in the database
	- you plan to write complex queries
- Snowflake (PAID SERVICE)
	- all the reasons from SQL point
	- you want a very fast result or your data is massive
	- you are also happy with rewriting your query to fit Snowflake SQL standards		
- Redshift (PAID SERVICE)
	- all the reasons from SQL point
	- you want a very fast result or your data is massive
	- you are also happy with rewriting your query to fit Redshift SQL standards			
- BigQuery (PAID SERVICE)
	- all the reasons from SQL point
	- you want a very fast result or your data is massive
	- you are happy to use geographical coordinates
	- you are also happy with rewriting your query to fit BigQuery SQL standards	
- Athena (PAID SERVICE)
	- all the reasons from SQL point
	- your data is massive
	- you are also happy with rewriting your query to fit Athena SQL standards
	- you are ok with execution times similar to postgres, if the data fits postgres
- Shapely 
	- You don't want to use Geopandas and you want to stay within Python
- Scala/Kotlin
	- the data fits the memory
	- you need to write a complex program of which KNN is just a part and you need it to run fast
- Rust 
	- all the reasons in Scala/Kotlin point
	- Scala/Kotlin wasn't wast enough
	- you are happy to deal with a reduced universe of libraries
- Pyspark
	- your data is massive and needs to be spread on different clusters