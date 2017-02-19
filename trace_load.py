## #!/usr/lib/spark/bin/spark-submit  

## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell

## node2trace.py           create taxonomy table, really wide, using dataframe
## trace_load.py	   load parquet stored by node2trace.py


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
        outfile = "/home/bofh/pub/trace_load_1108.out"
        outFH = open( outfile, 'w' )
        #outFH.write( "Test output to file from spark\n" )
        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark 
        spkCtx = SparkContext( appName='tin_pyspark_yarn_trace_load_1108' )

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
        #trace_table = sqlContext.read.parquet("trace_table.parquet")
        # above didn't work in c3po.  but stripping .parquet worked
        trace_table = sqlContext.read.parquet("trace_table")

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
        # run time on c3po with pe 16 and 16 GB e/a is also ~50 sec (w/ show).  
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




# main of program
load_trace_table()


# sample output
"""

select output from sprk-pe16x16.o2635024


17/02/18 23:35:31 INFO DAGScheduler: ResultStage 0 (parquet at NativeMethodAccessorImpl.java:-2) finished in 3.161 s
17/02/18 23:35:31 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
17/02/18 23:35:31 INFO DAGScheduler: Job 0 finished: parquet at NativeMethodAccessorImpl.java:-2, took 3.427512 s

root
 |-- taxid_T0: string (nullable = true)
 |-- name_T0: string (nullable = true)
 |-- parent_T0: string (nullable = true)
 |-- rank_T0: string (nullable = true)
 |-- taxid_T1: string (nullable = true)
 |-- name_T1: string (nullable = true)
 |-- parent_T1: string (nullable = true)
 |-- rank_T1: string (nullable = true)
 |-- taxid_T2: string (nullable = true)
 |-- name_T2: string (nullable = true)
 |-- parent_T2: string (nullable = true)
 |-- rank_T2: string (nullable = true)
 |-- taxid_T3: string (nullable = true)
 |-- name_T3: string (nullable = true)
 |-- parent_T3: string (nullable = true)
 |-- rank_T3: string (nullable = true)
 |-- taxid_T4: string (nullable = true)
 |-- name_T4: string (nullable = true)
 |-- parent_T4: string (nullable = true)
 |-- rank_T4: string (nullable = true)
 |-- taxid_T5: string (nullable = true)
 |-- name_T5: string (nullable = true)
 |-- parent_T5: string (nullable = true)
 |-- rank_T5: string (nullable = true)
 |-- taxid_T6: string (nullable = true)
 |-- name_T6: string (nullable = true)
 |-- parent_T6: string (nullable = true)
 |-- rank_T6: string (nullable = true)
 |-- taxid_T7: string (nullable = true)
 |-- name_T7: string (nullable = true)
 |-- parent_T7: string (nullable = true)
 |-- rank_T7: string (nullable = true)
 |-- taxid_T8: string (nullable = true)
 |-- name_T8: string (nullable = true)
 |-- parent_T8: string (nullable = true)
 |-- rank_T8: string (nullable = true)
 |-- taxid_T9: string (nullable = true)
 |-- name_T9: string (nullable = true)
 |-- parent_T9: string (nullable = true)
 |-- rank_T9: string (nullable = true)
 |-- taxid_T10: string (nullable = true)
 |-- name_T10: string (nullable = true)
 |-- parent_T10: string (nullable = true)
 |-- rank_T10: string (nullable = true)
 |-- taxid_T11: string (nullable = true)
 |-- name_T11: string (nullable = true)
 |-- parent_T11: string (nullable = true)
 |-- rank_T11: string (nullable = true)
 |-- taxid_T12: string (nullable = true)
 |-- name_T12: string (nullable = true)
 |-- parent_T12: string (nullable = true)
 |-- rank_T12: string (nullable = true)
 |-- taxid_T13: string (nullable = true)
 |-- name_T13: string (nullable = true)
 |-- parent_T13: string (nullable = true)
 |-- rank_T13: string (nullable = true)
 |-- taxid_T14: string (nullable = true)
 |-- name_T14: string (nullable = true)
 |-- parent_T14: string (nullable = true)
 |-- rank_T14: string (nullable = true)

+--------+--------------------+---------+-------+--------+--------------------+---------+-------+--------+----------+---------+-------+--------+-----------+---------+-----------+--------+-------------+---------+--------+--------+----------+---------+-------+--------+--------+---------+--------+--------+---------+---------+-------+--------+---------+---------+---------+--------+------------+---------+-------+---------+-----------+----------+--------+---------+----------+----------+--------+---------+-------------+----------+--------+---------+---------+----------+--------+---------+-----------+----------+--------+
|taxid_T0|             name_T0|parent_T0|rank_T0|taxid_T1|             name_T1|parent_T1|rank_T1|taxid_T2|   name_T2|parent_T2|rank_T2|taxid_T3|    name_T3|parent_T3|    rank_T3|taxid_T4|      name_T4|parent_T4| rank_T4|taxid_T5|   name_T5|parent_T5|rank_T5|taxid_T6| name_T6|parent_T6| rank_T6|taxid_T7|  name_T7|parent_T7|rank_T7|taxid_T8|  name_T8|parent_T8|  rank_T8|taxid_T9|     name_T9|parent_T9|rank_T9|taxid_T10|   name_T10|parent_T10|rank_T10|taxid_T11|  name_T11|parent_T11|rank_T11|taxid_T12|     name_T12|parent_T12|rank_T12|taxid_T13| name_T13|parent_T13|rank_T13|taxid_T14|   name_T14|parent_T14|rank_T14|
+--------+--------------------+---------+-------+--------+--------------------+---------+-------+--------+----------+---------+-------+--------+-----------+---------+-----------+--------+-------------+---------+--------+--------+----------+---------+-------+--------+--------+---------+--------+--------+---------+---------+-------+--------+---------+---------+---------+--------+------------+---------+-------+---------+-----------+----------+--------+---------+----------+----------+--------+---------+-------------+----------+--------+---------+---------+----------+--------+---------+-----------+----------+--------+
|   84300|         Bairdia sp.|    84299|species|   84299|             Bairdia|    84298|  genus|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
|  320506|Neonesidea haikan...|   221390|species|  221390|          Neonesidea|    84298|  genus|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
|  221395|Neonesidea sp. B-...|   221390|species|  221390|          Neonesidea|    84298|  genus|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
|  221394|Neonesidea sp. A-...|   221390|species|  221390|          Neonesidea|    84298|  genus|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1684349|Bairdiidae sp. 59...|  1642545|species| 1642545|Unclassified Bair...|    84298|no rank|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1684347|Bairdiidae sp. 4 ...|  1642545|species| 1642545|Unclassified Bair...|    84298|no rank|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1684346|Bairdiidae sp. 14...|  1642545|species| 1642545|Unclassified Bair...|    84298|no rank|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1642546|Bairdiidae sp. NVS25|  1642545|species| 1642545|Unclassified Bair...|    84298|no rank|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1642549|Bairdiidae sp. NVS27|  1642545|species| 1642545|Unclassified Bair...|    84298|no rank|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1684350|Bairdiidae sp. 60...|  1642545|species| 1642545|Unclassified Bair...|    84298|no rank|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1684348|Bairdiidae sp. 5 ...|  1642545|species| 1642545|Unclassified Bair...|    84298|no rank|   84298|Bairdiidae|    84320| family|   84320|Bairdioidea|    84319|superfamily|   84319|Bairdiocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
|  231262|Cyprididae sp. JM...|   231261|species|  231261|unclassified Cypr...|    43954|no rank|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1646078|Cyprididae sp. BO...|   231261|species|  231261|unclassified Cypr...|    43954|no rank|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
|  288915|Cyprididae sp. CH...|   231261|species|  231261|unclassified Cypr...|    43954|no rank|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1620910|Cyprididae gen. s...|   231261|species|  231261|unclassified Cypr...|    43954|no rank|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1775588|Ilyodromus sp. RJ...|  1775584|species| 1775584|          Ilyodromus|    43954|  genus|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1775587|Ilyodromus sp. RJ...|  1775584|species| 1775584|          Ilyodromus|    43954|  genus|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1775586|Ilyodromus sp. RJ...|  1775584|species| 1775584|          Ilyodromus|    43954|  genus|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
| 1646103| Notodromas sp. BIO1|   399044|species|  399044|          Notodromas|    43954|  genus|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
|  399045|  Notodromas monacha|   399044|species|  399044|          Notodromas|    43954|  genus|   43954|Cyprididae|    84329| family|   84329|Cypridoidea|    84328|superfamily|   84328|Cypridocopina|    84318|suborder|   84318|Podocopida|    43953|  order|   43953|Podocopa|     6670|subclass|    6670|Ostracoda|     6657|  class|    6657|Crustacea|   197562|subphylum|  197562|Pancrustacea|   197563|no rank|   197563|Mandibulata|      6656| no rank|     6656|Arthropoda|     88770|  phylum|    88770|Panarthropoda|   1206794| no rank|  1206794|Ecdysozoa|     33317| no rank|    33317|Protostomia|     33213| no rank|
+--------+--------------------+---------+-------+--------+--------------------+---------+-------+--------+----------+---------+-------+--------+-----------+---------+-----------+--------+-------------+---------+--------+--------+----------+---------+-------+--------+--------+---------+--------+--------+---------+---------+-------+--------+---------+---------+---------+--------+------------+---------+-------+---------+-----------+----------+--------+---------+----------+----------+--------+---------+-------------+----------+--------+---------+---------+----------+--------+---------+-----------+----------+--------+
only showing top 20 rows

  * (*) * running SQL Query COUNT... 
sqlResult is: %s DataFrame[_c0: bigint]
  * (*) * running SQL Query SELECT... 

myList is: %s [Row(taxid_T0=u'287144', name_T0=u'Influenza A virus (A/Taiwan/2040/2003(H3N2))', parent_T0=u'119210', rank_T0=u'no rank', taxid_T1=u'119210', name_T1=u'H3N2 subtype', parent_T1=u'11320', rank_T1=u'no rank', taxid_T2=u'11320', name_T2=u'Influenza A virus', parent_T2=u'197911', rank_T2=u'species', taxid_T3=u'197911', name_T3=u'Influenzavirus A', parent_T3=u'11308', rank_T3=u'genus', taxid_T4=u'11308', name_T4=u'Orthomyxoviridae', parent_T4=u'35301', rank_T4=u'family', taxid_T5=u'35301', name_T5=u'ssRNA negative-strand viruses', parent_T5=u'439488', rank_T5=u'no rank', taxid_T6=u'439488', name_T6=u'ssRNA viruses', parent_T6=u'10239', rank_T6=u'no rank', taxid_T7=u'10239', name_T7=u'Viruses', parent_T7=u'1', rank_T7=u'superkingdom', taxid_T8=u'1', name_T8=u'root', parent_T8=u'1', rank_T8=u'no rank', taxid_T9=u'1', name_T9=u'root', parent_T9=u'1', rank_T9=u'no rank', taxid_T10=u'1', name_T10=u'root', parent_T10=u'1', rank_T10=u'no rank', taxid_T11=u'1', name_T11=u'root', parent_T11=u'1', rank_T11=u'no rank', taxid_T12=u'1', name_T12=u'root', parent_T12=u'1', rank_T12=u'no rank', taxid_T13=u'1', name_T13=u'root', parent_T13=u'1', rank_T13=u'no rank', taxid_T14=u'1', name_T14=u'root', parent_T14=u'1', rank_T14=u'no rank')]
sqlResult is: %s DataFrame[taxid_T0: string, name_T0: string, parent_T0: string, rank_T0: string, taxid_T1: string, name_T1: string, parent_T1: string, rank_T1: string, taxid_T2: string, name_T2: string, parent_T2: string, rank_T2: string, taxid_T3: string, name_T3: string, parent_T3: string, rank_T3: string, taxid_T4: string, name_T4: string, parent_T4: string, rank_T4: string, taxid_T5: string, name_T5: string, parent_T5: string, rank_T5: string, taxid_T6: string, name_T6: string, parent_T6: string, rank_T6: string, taxid_T7: string, name_T7: string, parent_T7: string, rank_T7: string, taxid_T8: string, name_T8: string, parent_T8: string, rank_T8: string, taxid_T9: string, name_T9: string, parent_T9: string, rank_T9: string, taxid_T10: string, name_T10: string, parent_T10: string, rank_T10: string, taxid_T11: string, name_T11: string, parent_T11: string, rank_T11: string, taxid_T12: string, name_T12: string, parent_T12: string, rank_T12: string, taxid_T13: string, name_T13: string, parent_T13: string, rank_T13: string, taxid_T14: string, name_T14: string, parent_T14: string, rank_T14: string]
   *** good bye world !!
"""
