## #!/usr/lib/spark/bin/spark-submit  

## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell

## node2trace.py       create taxonomy table, really wide, using dataframe
## trace_load.py       load parquet stored by node2trace.py


from __future__         import print_function
from pyspark            import SparkContext
from pyspark.sql        import SQLContext, Row
from pyspark.sql.types  import *



# in  file: tree*csv                                 # taxonomy tree input
# out file = "/home/bofh/pub/node2trace_1107.out"    # output from various print cmd
# out file: see: save("trace_table", "parquet")      # data table output (cwd of job)
# create a wide taxonomy trace table by repeated self join of node ID with parent ID.
def create_trace_table() :
        outfile = "/home/bofh/pub/node2trace_1107.out"
        outFH = open( outfile, 'w' )
        #outFH.write( "Test output to file from spark\n" )
        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark 
        ##spkCtx = SparkContext( 'local', 'tin_pyspark_0724_local' )
        spkCtx = SparkContext( appName='tin_pyspark_yarn_node2trace_1107' )
        #print( "   *** hello world sparkContext created" )
        #print( "   *** hello world sparkContext created", file = outFH )

        sqlContext = SQLContext(spkCtx)         # can only have one instance of SQLContext
        #tabABsqlCtx = SQLContext(spkCtx)       # a second delcaration will result in not so useful object
        print( "   *** hello world spark sql context created" )
        print( "   *** hello world spark sql context created", file = outFH )

        treeSqlCtx = spkCtx.textFile("tree.table.csv")  # taxonomy tree table (this is req input file)
        partsT = treeSqlCtx.map(lambda l: l.split(","))
        treeTab = partsT.map(lambda p: (p[0], p[1].strip('"'), p[2].strip(), p[3].strip('"')))    
        schemaStringT = "taxid name parent rank"
        fieldsT = [StructField(field_name, StringType(), True) for field_name in schemaStringT.split()]
        schemaT = StructType(fieldsT)
        treeDF  = sqlContext.createDataFrame(treeTab,schemaT) 
        #treeDF.printSchema()
        #treeDF.show()

        # seeding an initial table before any join
        tabT = []
        tabT.append(treeDF)     
        tabT[0] = treeDF.withColumnRenamed("taxid", "taxid_T0").withColumnRenamed("name","name_T0").withColumnRenamed("parent","parent_T0").withColumnRenamed("rank","rank_T0")
        #tabT[0].show()


        # join tree table with itself repeatedly to get parent info, forming a lineage in each row
        lastIdx = 15
        for j in range(1,lastIdx):    # range(2,3) produces [2], so exclude last index.  think of calculus [2,3)
            i = j - 1                 # i comes before j, and i has 1 less than j
            #print( "running i = %s" % i ) 
            print( "running j = %s" % j ) 
            fieldname = "parent_T%s" % i
            #print( "fieldname is set to %s" % fieldname ) 
            #                                           *** T1 is wrong below .   need something dynamic, equiv to T[i]
            #tabT[j] = tabT[j].join(treeDF, tabT[i].parent_T1 == treeDF.taxid, "inner")
            tabT.append(treeDF)
            tabT[j] = tabT[i].join(treeDF, getattr(tabT[i], fieldname) == treeDF.taxid, "inner") # http://stackoverflow.com/questions/31843669/how-to-pass-an-argument-to-a-function-that-doesnt-take-string-pyspark
            tabT[j] = tabT[j].withColumnRenamed("taxid", "taxid_T%s" % j).withColumnRenamed("name","name_T%s" % j).withColumnRenamed("parent","parent_T%s" % j).withColumnRenamed("rank","rank_T%s" % j)
            #tabT[j].show()      
        #end for loop

        # for example output, see trace_load.py

        # saving data for future use... 
        #tabT[lastIdx-1].show()      
        #tabT[lastIdx-1].collect()       # lastIdx=15, .collect() crashed!!
        ## output file: saving to parquet file.  loc: cwd of job.  it is actually a dir with many files
        tabT[lastIdx-1].select("*").save("trace_table", "parquet")      # parquet format seems to be the default.  took 3.1 min
        # https://spark.apache.org/docs/1.5.2/sql-programming-guide.html#generic-loadsave-functions     # Generic Load/Save
        # above save works, but need to ensure col names are uniq

        ## printing rdd:
        ## https://spark.apache.org/docs/latest/programming-guide.html#printing-elements-of-an-rdd
        ## maybe of interest in removing extra column...
        ## https://blogs.msdn.microsoft.com/azuredatalake/2016/02/10/pyspark-appending-columns-to-dataframe-when-dataframe-withcolumn-cannot-be-used/

        tabT[lastIdx-1].registerTempTable("trace_table")
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

        print( "   *** good bye world !!" )
        print( "   *** good bye world !!", file = outFH )
        outFH.close()
        exit 
#end create_trace_table() 



# main execution of this file
create_trace_table() 


