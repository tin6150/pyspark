#!/usr/lib/spark/bin/pyspark

from pyspark import SparkContext
sc = SparkContext( 'local', 'pyspark' )
print( "hello world from pyspark" )

text = sc.textFile("passwd.txt")
print( text )


