#!/usr/lib/spark/bin/pyspark


## warning message says to use
# /usr/lib/spark/bin/spark-submit  spark_2.py


from pyspark import SparkContext
from operator import add

sc = SparkContext( 'local', 'pyspark' )
print( "  *** hello world from pyspark" )

text = sc.textFile("passwd.txt")
#text = sc.textFile("/home/hoti1/code/svn/spark/passwd.txt")
print( text )

# https://districtdatalabs.silvrback.com/getting-started-with-spark-in-python

def tokenize(text) :
	return text.split(":")

words = text.flatMap(tokenize)
print(words)

wc = words.map(lambda x: (x,1))
print wc.toDebugString() 		# things aren't quite working yet
				# expect file at hdfs://clp24/home/hoti1/code/svn/spark/passwd.txt
print( wc )

counts = wc.reduceByKey(add)		# comain: hdfs://clp24/user/hoti1/passwd.txt does not exist...
counts.saveAsTextFile("wc.txt")






print( "  *** bye bye world" )
