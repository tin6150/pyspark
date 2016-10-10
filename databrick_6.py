#!/usr/lib/spark/bin/pyspark


## warning message says to use
# /usr/lib/spark/bin/spark-submit  spark_2.py


from pyspark import SparkContext
from pyspark import SQLContext

sc = SparkContext( 'local', 'pyspark' )
print( "  *** hello world from pyspark" )

## https://databricks-training.s3.amazonaws.com/data-exploration-using-spark-sql.html
sqlCtx = SQLContext(sc)

print( "   *** hello world spark sql context created" )


#wikiData = sqlCtx.parquetFile("data/wiki_parquet")
wikiData = sqlCtx.parquetFile("password.txt")

# get  error:
# maybe file was spooled to hdfs, but unable to process it correctly?
# py4j.protocol.Py4JJavaError: An error occurred while calling o36.parquetFile.
#: java.lang.AssertionError: assertion failed: No predefined schema found, and no Parquet data files or summary files found under hdfs://clp24/user/hoti1/password.txt.




wikiDAta.count()
wikiData.registerAsTable("wikiData")
result = sqlCtx.sql("SELECT COUNT(*) AS pageCount FROM wikiData").collect()

print result[0].pageCount


print( "  *** bye bye world" )
