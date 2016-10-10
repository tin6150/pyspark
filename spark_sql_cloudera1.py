#!/usr/lib/spark/bin/pyspark


## warning message says to use
# /usr/lib/spark/bin/spark-submit  spark_...py


#from pyspark import SparkContext
#from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext



sc = SparkContext( 'local', 'pyspark' )
print( "   *** hello world sparkContext created" )

sqlContext = SQLContext(sc)
print( "   *** hello world spark sql context created" )




## http://www.cloudera.com/documentation/enterprise/5-6-x/topics/spark_sparksql.html

# the following work in spark-shell, which give a scala prompt, 
# but don't work inside this script
# need some more stuff declared before can use sqlContext??

#sqlContext.sql("CREATE TABLE sample_07 (code string,description string,total_emp int,salary int)                       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")
sqlContext.sql("CREATE TABLE passwd_2  (user string,shadow string,uid int,gid int,desc string,home string,shell string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ':'  STORED AS TextFile")

# the passwd.txt file is inside the hdfs dfs
sqlContext.sql("LOAD DATA INPATH 'passwd.txt' OVERWRITE INTO TABLE passwd")


# now get this, something wront with no tmp?
# 16/06/06 22:28:41 ERROR Driver: FAILED: SemanticException [Error 10028]: Line 1:17 Path is not legal ''passwd.txt'': Move from: hdfs://clp24/user/hoti1/passwd.txt to: file:/tmp/spark-4100eebe-5c8f-49ef-b1ab-fd88e5902b6a/metastore/passwd is not valid. Please check that values for params "default.fs.name" and "hive.metastore.warehouse.dir" do not conflict.
