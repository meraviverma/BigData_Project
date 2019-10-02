package SparkDefGuide.Chapter9

import conf.SparkConfiguration
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
/*Data Source
Read API Structure
---------------------
//DataFrameReader.format(.....).option("key","value").schema(....).load()
format is optional because by default Spark will use the Parquet format.

readMode 		    Description
---------		    ---------------
permissive 		  Sets all fields to null when it encounters a corrupted record.
dropMalformed 	Drops the row that contains malformed records.
failFast 		    Fails immediately upon encountering malformed records.

Write API Structure
----------------------
DataFrameWriter.format(...).option(...).partition(...).bucketBy(...).sortBy(...).save()
saveMode 		    Description
---------		    ---------------
append			    Appends the output files to the list of files that already exist at that location.
overwrite 		  Will completely overwrite any data that already exists there.
errorIfExists	  Throws an error and fails the write if data or files already exist at the specified location.
ignore			    If data or files exist at the location, do nothing with the current DataFrame.
*/
object Chapter9 extends  SparkConfiguration{
  def main(args: Array[String]): Unit = {

    /*
    spark.read.format("csv")
      .option("mode","FAILFAST")
      .option("inferSchema","true")
      .option("path","/path/to/file")
      .schema(someschema)
      .load()

    dataframe.write.format("csv")
	    .option("mode", "OVERWRITE")
	    .option("dateFormat", "yyyy-MM-dd")
	    .option("path","path/to/file(s)")
	    .save("path here")
     */

    val myManualSchema=new StructType(Array(
      new StructField("DEST_COUNTRY_NAME",StringType,true),
      new StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      new StructField("count",LongType,false)
    ))

    sc.read.format("csv")
      .option("header","true")
      .option("mode","FAILFAST")
      .schema(myManualSchema)
      .load("D:\\mypro\\spark\\data-sparkguide\\2010-summary.csv")
      .show(5)

    println("########## Reading JSON File ###############")
    //multiline, when you set this option to true, you can read the entire file as one json object
    //and Spark will go through the work of parsing that into a DataFrame

    sc.read.format("json")
      .option("mode","FAILFAST")
      .schema(myManualSchema)
      .load("D:\\mypro\\spark\\data-sparkguide\\2010-summary.json").show(5)

    println("############## Parquet File ####################3")
    /*Parquet file ia an open source column-oriented data source that provides a variety of
    storage optimization. It provides columnar compression, which saves storage space and allow for reading individual
    columns instead of entire files
    Recommended to writing data out of parquet for long term storage because reading from a parquet file will
    always be more efficient than JSON or CSV.
    It supports complex types

    */

    /*sc.read.format("parquet")
      .load("")*/
    println("######### ORC FILE ###############")
    //fundamental differencce betweeen ORC and parquet is that parquet is further optimized for use with
    //spark whereas ORC is further optimized for Hive.

    println("########### Text Files ################")
    sc.read.textFile("D:\\mypro\\spark\\data-sparkguide\\2010-summary.csv")
      .selectExpr("split(value,',') as rows").show()

    /*
    Writing text file :- If you perform some partitioning when performing your write you can write more
    columns, however, these columns will manifest as directories in the folder to folder which you are
    writing out

    csvFile.limit(10).select("DEST_COUNTRY_NAME","count").write.partitionBy("count").text("/tmp/file-csv.csv")

   Writing data in parallel :- one file is written per partition of data
   csvFile.repartition(5).write.format("csv").save("/tmp/mult.csv")

   PARTITIONING:-
   csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME").save("/tmp/par.parquet")

   BUCKETING:-
   Control the data that is specially written to each file.This can help avoid shuffles later when you go
   to read the data because data with the same bucket ID will be grouped together into one physical partition

   val numberBucket=10
   val columnToBucketBy="count"

   csvFile.write.format("parquet").mode("overwrite")
     .bucketBy(numberBucket,columnToBucketBy).saveAsTable("bucketedFiles")

     maxRecordsPerFile

     df.write.option("maxRecordsperFile",5000) :- spark will ensure that files will contain at most 5,000 records
     */




    }
  }
