## #!/usr/lib/spark/bin/pyspark 

## warning say to use spark-submit  spark_acc2taxid.py   
## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell...

## spark_acc2taxid_3.py    this version does sql lookup one row at a time.  works, but slow.
## spark_acc2taxid_4.py    this version use join on dataframe, should be done in ~5 min as per Victor Hong estimate
## node2trace.py           renamed file, as this will now focus only in creating a table with full parent lineage 
##                         ie, DW-style pre-computing the lineage tree.

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

def create_trace_table() :
        outfile = "/home/hoti1/pub/node2trace_1107.out"
        #outfile = "/home/hoti1/pub/spark_acc2taxid_4.out"
        outFH = open( outfile, 'w' )
        #outFH.write( "Test output to file from spark\n" )
        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark 
        ##spkCtx = SparkContext( 'local', 'tin_pyspark_0724_local' )
        #spkCtx = SparkContext( appName='tin_pyspark_31gb' )
        spkCtx = SparkContext( appName='tin_pyspark_yarn_node2trace_1107' )
        #print( "   *** hello world sparkContext created" )
        #print( "   *** hello world sparkContext created", file = outFH )

        sqlContext = SQLContext(spkCtx)         # can only have one instance of SQLContext
        #tabABsqlCtx = SQLContext(spkCtx)       # a second delcaration will result in not so useful object
        print( "   *** hello world spark sql context created" )
        print( "   *** hello world spark sql context created", file = outFH )

        treeSqlCtx = spkCtx.textFile("tree.table.csv")  # tree table 
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
        tabT.append(treeDF)     # create tabT[0], didn't have this before, just used tabTL...
        #tabTL = treeDF.withColumnRenamed("taxid", "taxid_T0").withColumnRenamed("name","name_T0").withColumnRenamed("parent","parent_T0").withColumnRenamed("rank","rank_T0")
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
        #print( "outside for loop" )# 
        #tabT[lastIdx-1].printSchema() 
        #tabT[lastIdx-1].show()                  # works, careful not to go beyond array element not yet created in for-loop
        # for-loop just declare all conditions to spark, the show() is when work get executed.  join up to lastIdxi=15 takes 2.5 min
        # count distinct below shows 1,402,066 rows.  took  maybe 15 sec, 


        """
            root
             |-- taxid: string (nullable = true)
             |-- name: string (nullable = true)
             |-- parent: string (nullable = true)
             |-- rank: string (nullable = true)

            root
             |-- taxid_T0: string (nullable = true)
             |-- name_T0: string (nullable = true)
             |-- parent_T0: string (nullable = true)
             |-- rank_T0: string (nullable = true)
             |-- taxid_T1: string (nullable = true)
             |-- name_T1: string (nullable = true)
             |-- parent_T1: string (nullable = true)
             |-- rank_T1: string (nullable = true)

            +--------+--------------------+---------+-------+--------+----------+---------+-------+
            |taxid_T0|             name_T0|parent_T0|rank_T0|taxid_T1|   name_T1|parent_T1|rank_T1|
            +--------+--------------------+---------+-------+--------+----------+---------+-------+
            |  100459|   Miliaria calandra|   100458|species|  100458|  Miliaria|   400781|  genus|
            | 1542402|Ophiopogon hetera...|   100502|species|  100502|Ophiopogon|   703537|  genus|
            | 1542407|Ophiopogon multif...|   100502|species|  100502|Ophiopogon|   703537|  genus|
            | 1542404|Ophiopogon longib...|   100502|species|  100502|Ophiopogon|   703537|  genus|
            | 1542408|Ophiopogon pelios...|   100502|species|  100502|Ophiopogon|   703537|  genus|

        
+--------+--------------------+---------+----------+--------+--------------------+---------+-------+--------+------------+---------+-------+
|taxid_T0|             name_T0|parent_T0|   rank_T0|taxid_T1|             name_T1|parent_T1|rank_T1|taxid_T2|     name_T2|parent_T2|rank_T2|
+--------+--------------------+---------+----------+--------+--------------------+---------+-------+--------+------------+---------+-------+
|  505316|Miliaria calandra...|   100459|subspecies|  100459|   Miliaria calandra|   100458|species|  100458|    Miliaria|   400781|  genus|
|  100508|Ophiopogon japoni...|   100506|  varietas|  100506|Ophiopogon japonicus|   100502|species|  100502|  Ophiopogon|   703537|  genus|
|  100507|Ophiopogon japoni...|   100506|  varietas|  100506|Ophiopogon japonicus|   100502|species|  100502|  Ophiopogon|   703537|  genus|
| 1542395|Ophiopogon bodini...|   235913|  varietas|  235913|Ophiopogon bodinieri|   100502|species|  100502|  Ophiopogon|   703537|  genus|
| 1542394|Ophiopogon bockia...|  1468144|  varietas| 1468144|Ophiopogon bockianus|   100502|species|  100502|  Ophiopogon|   703537|  genus|
| 1542396|Ophiopogon chingi...|   235914|  varietas|  235914|  Ophiopogon chingii|   100502|species|  100502|  Ophiopogon|   703537|  genus|
|  559883|  Snake adenovirus 3|  1146872|   no rank| 1146872|  Snake adenovirus A|   100953|species|  100953|Atadenovirus|    10508|  genus|
|  559881|  Snake adenovirus 2|  1146872|   no rank| 1146872|  Snake adenovirus A|   100953|species|  100953|Atadenovirus|    10508|  genus|
|  554450|Viperid adenoviru...|  1146872|   no rank| 1146872|  Snake adenovirus A|   100953|species|  100953|Atadenovirus|    10508|  genus|
|  189830|  Snake adenovirus 1|  1146872|   no rank| 1146872|  Snake adenovirus A|   100953|species|  100953|Atadenovirus|    10508|  genus|
|  576945|Viperid adenoviru...|  1146872|   no rank| 1146872|  Snake adenovirus A|   100953|species|  100953|Atadenovirus|    10508|  genus|
| 1147765|Snake adenovirus ...|  1146872|   no rank| 1146872|  Snake adenovirus A|   100953|species|  100953|Atadenovirus|    10508|  genus|
|  130500| Bovine adenovirus E|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
| 1036586|Western bearded d...|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
| 1036587|Central netted dr...|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
|  332202|Agamid atadenovir...|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
|  332203|Agamid atadenovir...|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
|  270906|Helodermatid aden...|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
|  270904| Agamid adenovirus 1|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
|  270903|Eublepharid adeno...|   327833|   species|  327833|unclassified Atad...|   100953|no rank|  100953|Atadenovirus|    10508|  genus|
+--------+--------------------+---------+----------+--------+--------------------+---------+-------+--------+------------+---------+-------+
        """

        # saving data for future use... 
        # if only takes 3 min to generate, maybe not worth saving? 
        #tabT[lastIdx-1].show()      
        #tabT[lastIdx-1].collect()       # lastIdx=15, .collect() crashed!!
        tabT[lastIdx-1].select("*").save("trace_table", "parquet")      # parquet format seems to be the default.  took 3.1 min
        # https://spark.apache.org/docs/1.5.2/sql-programming-guide.html#generic-loadsave-functions     # Generic Load/Save
        # above save works, but need to ensure col names are uniq

        ## printing rdd:
        ## https://spark.apache.org/docs/latest/programming-guide.html#printing-elements-of-an-rdd
        ## maybe of interest in removing extra column...
        ## https://blogs.msdn.microsoft.com/azuredatalake/2016/02/10/pyspark-appending-columns-to-dataframe-when-dataframe-withcolumn-cannot-be-used/


        # running some sample SQL queries for sanity check (works, but not really needed anymore)
        # may want to try to query and show this record
        # |taxid_T0|             name_T0|parent_T0|rank_T0|
        # |  287144|Influenza A virus...|   119210|no rank|
        # for plant, even lastIdx=15 wasn't enough to get the full lineage trace
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



create_trace_table() 


