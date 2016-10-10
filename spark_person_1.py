#!/usr/lib/spark/bin/pyspark


## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell...



from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext( 'local', 'pyspark' )
print( "   *** hello world sparkContext created" )

sqlContext = SQLContext(sc)
print( "   *** hello world spark sql context created" )


#df = sqlContext.read.json("./taxorpt.json")
#df.show()

# https://spark.apache.org/docs/latest/sql-programming-guide.html#getting-started
##lines = sc.textFile("passwd.txt")
#lines = sc.textFile("/home/hoti1/code/svn/spark/example/people.json")
#lines = sc.textFile("example/people.json")
#parts = lines.map(lambda l: l.split(","))
#people = parts.map(lambda p: (p[0], p[1].strip()))

#schemaString = "name age"

# http://spark.apache.org/docs/latest/sql-programming-guide.html#inferring-the-schema-using-reflection

# file person.txt is in hdfs.  all reference to file are to hdfs, not unix file system!
lines = sc.textFile("person.txt")
parts = lines.map(lambda l: l.split(","))
#people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
people = parts.map(lambda p: Row(name=p[0], ageUid=int(p[1])))

schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

teenagers = sqlContext.sql( "SELECT name from people WHERE ageUid >= 10 AND ageUid <=400" )

teenNames = teenagers.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
        print(teenName)

## up to here worked so far!!   :)  
##

print( "   *** good bye world !!" )
