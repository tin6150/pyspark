#!/usr/lib/spark/bin/pyspark


## warning message says to use
# /usr/lib/spark/bin/spark-submit  spark_5.py

# https://districtdatalabs.silvrback.com/getting-started-with-spark-in-python

from pyspark import SparkConf, SparkContext
#from operator import add

print( "  *** hello world" )

APP_NAME = "Sn Spark App"

def main(sc) :
	pass

if __name__ == "__main__" :
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)

	main(sc)



print( "  *** bye bye world" )
