#!/usr/lib/spark/bin/pyspark


## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell...

# https://spark.apache.org/docs/latest/sql-programming-guide.html#getting-started

from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext( 'local', 'pyspark' )
print( "   *** hello world sparkContext created" )

sqlContext = SQLContext(sc)
print( "   *** hello world spark sql context created" )

df = sqlContext.read.json("./taxorpt.json")
df.show()

#lines = sc.textFile("passwd.txt")
lines = sc.textFile("/home/hoti1/code/svn/spark/example/people.json")
lines = sc.textFile("example/people.json")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))

schemaString = "name age"


print( "   *** good bye world !!" )
