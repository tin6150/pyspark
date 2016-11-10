## #!/usr/lib/spark/bin/pyspark 

## warning say to use spark-submit  spark_acc2taxid.py   
## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell...

## spark_acc2taxid_3.py    this version does sql lookup one row at a time.  works, but slow.
## spark_acc2taxid_4.py    this version use join on dataframe, should be done in ~5 min as per Victor Hong estimate
## node2trace.py           renamed file, as this will now focus only in creating a table with full parent lineage 
##                         ie, DW-style pre-computing the lineage tree.

## trace_load.py	   load parquet stored by node2trace.py
## 			   may eventually be added to join with acc_ver info


from __future__         import print_function
from pyspark            import SparkContext
from pyspark.sql        import SQLContext, Row
from pyspark.sql.types  import *

def dummy_writeFileTest() :
        outFH = open( outfile, 'w' )
        outFH.write( "typical write method\n" )
        print( "print redirect write method, need special from __future__ import in python2 to work" , file = outFH )
        outFH.close()
# end dummy


def load_trace_table() :
        outfile = "/home/hoti1/pub/trace_load_1108.out"
        #outfile = "/home/hoti1/pub/spark_acc2taxid_4.out"
        outFH = open( outfile, 'w' )
        #outFH.write( "Test output to file from spark\n" )
        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark 
        ##spkCtx = SparkContext( 'local', 'tin_pyspark_0724_local' )
        #spkCtx = SparkContext( appName='tin_pyspark_31gb' )
        spkCtx = SparkContext( appName='tin_pyspark_yarn_trace_load_1108' )
        #print( "   *** hello world sparkContext created" )
        #print( "   *** hello world sparkContext created", file = outFH )

        sqlContext = SQLContext(spkCtx)         # can only have one instance of SQLContext
        #tabABsqlCtx = SQLContext(spkCtx)       # a second delcaration will result in not so useful object
        print( "   *** hello world spark sql context created" )
        print( "   *** hello world spark sql context created", file = outFH )

        #treeSqlCtx = spkCtx.textFile("tree.table.csv")  # tree table 
        #partsT = treeSqlCtx.map(lambda l: l.split(","))
        #treeTab = partsT.map(lambda p: (p[0], p[1].strip('"'), p[2].strip(), p[3].strip('"')))    
        #schemaStringT = "taxid name parent rank"
        #fieldsT = [StructField(field_name, StringType(), True) for field_name in schemaStringT.split()]
        #schemaT = StructType(fieldsT)
        #treeDF  = sqlContext.createDataFrame(treeTab,schemaT) 
        #treeDF.printSchema()
        #treeDF.show()
        #treeDF = DataFrame      # ??

        #parquetFile = sqlContext.read.parquet("people.parquet")
        #parquetFile.registerTempTable("parquetFile");
        trace_table = sqlContext.read.parquet("trace_table.parquet")

        trace_table.printSchema()
        trace_table.show()


        # running some sample SQL queries for sanity check (works, but not really needed anymore)

        #tabT[lastIdx-1].registerTempTable("trace_table")
        trace_table.registerTempTable("trace_table")
        print( "  * (*) * running SQL Query COUNT... " )
        print( "  * (*) * running SQL Query COUNT... ", file = outFH )
        sqlResult = sqlContext.sql("SELECT COUNT( taxid_T0 ) FROM trace_table") # spark does NOT allow for ; at end of SQL !!
        print( "   *** sqlResult: ***", file = outFH )
        myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        print( "myList is: %s", myList )                        # [Row(_c0=1402066)]   # took about 20 sec (53sec if omit .show() after all the joins)
        print( "myList is: %s", myList, file = outFH )          # this works too! 
        print( "sqlResult is: %s", sqlResult )                  # sqlResult is: %s DataFrame[_c0: bigint]
        print( "sqlResult is: %s", sqlResult, file = outFH )
        #if( sqlResult.count() > 1 ):
        #        print( "Houston, we got more than one element returned!")

        # may want to try to query and show this record
        # |taxid_T0|             name_T0|parent_T0|rank_T0|
        # |  287144|Influenza A virus...|   119210|no rank|
        # for plant, even lastIdx=15 wasn't enough to get the full lineage trace


        print( "  * (*) * running SQL Query SELECT... " )
        print( "  * (*) * running SQL Query SELECT... ", file = outFH )
        #sqlResult = sqlContext.sql( "SELECT taxid from acc_taxid WHERE acc_ver = 'T02888.1' " )  # spark does NOT allow for ; at end of SQL !!
        sqlResult = sqlContext.sql( "SELECT * from trace_table WHERE taxid_T0 = '287144' " )  # spark does NOT allow for ; at end of SQL !!
        print( "   *** sqlResult: ***", file = outFH )
        myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        ## collect put all data into a single machine, so avoid running till the end for true big data
        print( "myList is: %s", myList )                        # [Row(_c0=1402066)]   # took about 20 sec (53sec if omit .show() after all the joins)
        print( "myList is: %s", myList, file = outFH )          # this works too! 
        print( "sqlResult is: %s", sqlResult )                  # sqlResult is: %s DataFrame[_c0: bigint]
        print( "sqlResult is: %s", sqlResult, file = outFH )

        #myList = sqlResult.collectAsMap()      # sqlResult is DF, not RDD, no collectAsMap fn.
        # sqlResult is DF, not RDD.  specifically, it is DataFrame[taxid: string]
        #myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        #print( myList[0].taxid )                # taxid is the name of the column specified in select
        #print( myList[0].taxid, file = outFH )  # taxid is the name of the column specified in select

        print( "   *** good bye world !!" )
        print( "   *** good bye world !!", file = outFH )
        outFH.close()
        exit 
# end load_trace_table()






load_trace_table()
