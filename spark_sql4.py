#!/usr/lib/spark/bin/pyspark

# https://spark.apache.org/docs/latest/sql-programming-guide.html#getting-started

from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext( 'local', 'pyspark' )
print( "   *** hello world sparkContext created" )

sqlContext = SQLContext(sc)
print( "   *** hello world spark sql context created" )

#df = sqlContext.read.json("./taxorpt.json")
#df.show()

#lines = sc.textFile("passwd.txt")
lines = sc.textFile("/home/hoti1/code/svn/spark/example/people.json")
lines = sc.textFile("example/people.json")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))

schemaString = "name age"

print( " *** something about StructField and StructType not working, may need more lib, not listed in eg " )

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
##fields =  schemaString.split()
schema = StructType(fields) 

schemaPeople = sqlContext.createDataFrame(people, schema)

schemaPeople.registerTemTable("people")

results = sqlContext.sql("SELECT name FROM people")

names = result.map(lambda p: "Name: " + p.name)
for name in names.collect() :
	print(name)


print( "   *** good bye world !!" )
