#!/usr/lib/spark/bin/pyspark

## warning say to use spark-submit  spark_acc2taxid.py   
## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell...



from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

sc = SparkContext( 'local', 'pyspark' )
print( "   *** hello world sparkContext created" )

sqlContext = SQLContext(sc)
print( "   *** hello world spark sql context created" )


#df = sqlContext.read.json("./taxorpt.json")
#df.show()

# https://spark.apache.org/docs/latest/sql-programming-guide.html#getting-started
##lines = sc.textFile("passwd.txt")
#lines = sc.textFile("example/people.json")
#parts = lines.map(lambda l: l.split(","))
#people = parts.map(lambda p: (p[0], p[1].strip()))

#schemaString = "name age"

# http://spark.apache.org/docs/latest/sql-programming-guide.html#inferring-the-schema-using-reflection

# file person.txt is in hdfs.  all reference to file are to hdfs, not unix file system!
#lines = sc.textFile("person.txt")
#lines = sc.textFile("nucl_gss.accession2taxid")                # 484.763253 sec to count(*), ie 8 min to cound 39,517,524 rows
lines = sc.textFile("nucl_gss.accession2taxid.head100")         # 0.550907 sec to count (*)
##  there is a sc.parallelize(...) fn, do i need to call that?  or map will make parallel?
parts = lines.map(lambda l: l.split("\t"))
#people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
#people = parts.map(lambda p: Row(name=p[0], ageUid=int(p[1])))
acc_taxid = parts.map(lambda p: (p[0], p[1].strip(), p[2].strip(), p[3].strip() ))

## spark itself don't seems to have sqlContext.read for csv, only json and parquet
## there are add on that can be used eg 
## http://www.sparktutorials.net/opening-csv-files-in-apache-spark---the-spark-data-sources-api-and-spark-csv

## so maybe better deal with this map syntax to rdd, then use .toDF to convert RDD to DataFrame
## https://www.mapr.com/blog/using-apache-spark-dataframes-processing-tabular-data

#schemaString = "name ageUid"
#schemaString = "accession accession_version taxid gi" 
schemaString = "acc acc_ver taxid gi" 
## CREATE TABLE acc_taxid(acc text, acc_ver text PRIMARY KEY, taxid integer, gi integer);


fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

#schemaPeople = sqlContext.createDataFrame(people)
schemaAccTaxid = sqlContext.createDataFrame(acc_taxid,schema)
schemaAccTaxid.registerTempTable("acc_taxid")
# consider saveAsTable to create persistence
# https://spark.apache.org/docs/latest/sql-programming-guide.html#saving-to-persistent-tables

print( "  ** learning about the schema setup... " )

## schemaAccTaxid is a dataFrame
## https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations
schemaAccTaxid.show()
schemaAccTaxid.printSchema()
schemaAccTaxid.select("acc_ver","taxid").show()
schemaAccTaxid.select("acc_ver", schemaAccTaxid["taxid"]*100).show()
schemaAccTaxid.groupBy("taxid").count().show()


print( "  ** running Query of  SELECT taxid from acc_taxid WHERE acc_ver = '...' " )
sqlResult = sqlContext.sql( "SELECT taxid from acc_taxid WHERE acc_ver = 'T02634.1' " )
#myList = sqlResult.collectAsMap()      # sqlResult is DF, not RDD, no collectAsMap fn.
# sqlResult is DF, not RDD.  specifically, it is DataFrame[taxid: string]
## collect put all data into a single machine, so avoid running till the end for true big data
myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
print( myList[0].taxid )                # taxid is the name of the column specified in select



print( "  ** running Query of  SELECT taxid from acc_taxid " )
sqlResult = sqlContext.sql( "SELECT * from acc_taxid" )     # DataFrame[taxid: string]
#print( "   *** sqlResult: ***" )
#print( sqlResult )

myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
print( myList[0].taxid )                # taxid is the name of the column specified in select
print( myList[1].taxid )                # taxid is the name of the column specified in select
print( myList[0].acc_ver ) # this maybe a header row
print( myList[1].acc_ver )

for x in myList:
        print( x )              # Row(acc=u'T02634', acc_ver=u'T02634.1', taxid=u'5833', gi=u'319156')
                                # Row is some special structure, which was import at top of script


print( "  ** running Query of  SELECT count(*) from acc_taxid " )
sqlResult = sqlContext.sql( "SELECT count(*) from acc_taxid" ) # data structure print as DataFrame[_c0: bigint]
sqlResult.show()        # this print a pretty formatted table
                        # note that column header for the coun(*) is called _c0 !!
# sqlResult is a dataframe, specifically: DataFrame[_c0: bigint]

if( sqlResult.count() > 1 ):
        print( "Houston, we got more than one element returned!")

myList = sqlResult.collect()    # need .collect() to consolidate result into "Row"
print( myList )                 # [Row(_c0=100)]
print( myList[0] )              #  Row(_c0=100)
print( myList[0]._c0 )          #          100    ie_c0 is the special name given to the count(*) table



# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.types.StringType
#myStr = sqlResult.simpleString()
#print( "DF to string %s" % myStr )
## see teenNames.collect for eg on how to convert DF back to "normal/scalar" variables.
#myStr = sqlResult.take(1)        
#print( myStr )

## some DF functions??
## https://www.mapr.com/blog/using-apache-spark-dataframes-processing-tabular-data 


#teenagers = sqlContext.sql( "SELECT name from people WHERE ageUid >= 10 AND ageUid <=400" )
#teenNames = teenagers.map(lambda p: "Nombre: " + p.name)  # teenNames is an RDD$
#        for teenName in teenNames.collect():           # collect() return a list of all elements in the RDD$
#        print(teenName)

## up to here worked so far!!   :)  
##

print( "   *** good bye world !!" )
