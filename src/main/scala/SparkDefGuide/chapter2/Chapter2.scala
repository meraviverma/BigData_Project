package SparkDefGuide.chapter2

import conf.SparkConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Chapter2 extends SparkConfiguration {

  def main(args: Array[String]): Unit = {
    //Returns a [[DataFrameReader]] that can be used to read non-streaming data in as a`DataFrame`.

    /*Specifies the schema by using the input DDL-formatted string. Some data sources (e.g. JSON) can
      infer the input schema automatically from data. By specifying the schema here, the underlying
      data source can skip the schema inference step, and thus speed up data loading.
    */
    val flightData2014: DataFrame =sc
      .read
      .option("inferSchema",true)
      .option("header",true)
      .csv("D:\\mypro\\spark\\data-sparkguide\\2014-summary.csv")

    flightData2014.show(3)

    flightData2014.createOrReplaceTempView("flightData2015")
    flightData2014.sort("count").explain()
    //explain plan are a bit arcane, read explain from top to bottom, the top being the end result, and the bottom being
    //the source data

    sc.conf.set("spark.sql.shuffle.partitions",5);

    val dataframeway=flightData2014.groupBy("DEST_COUNTRY_NAME").count()
    dataframeway.explain()

    val maxsql=sc.sql("select DEST_COUNTRY_NAME,sum(count) as destination_total from flightData2015 GROUP BY DEST_COUNTRY_NAME " +
      "ORDER BY sum(count) DESC LIMIT 5 ")

    maxsql.show()

    //NOTE: do this import org.apache.spark.sql.functions._ other wise sort(desc()) will not work
    flightData2014
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)","destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

  }
}
  //OUTPUT//
/*+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|       Saint Martin|    1|
|    United States|            Romania|   12|
|    United States|            Croatia|    2|
+-----------------+-------------------+-----+
only showing top 3 rows

== Physical Plan ==
*(2) Sort [count#12 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(count#12 ASC NULLS FIRST, 200)
   +- *(1) FileScan csv [DEST_COUNTRY_NAME#10,ORIGIN_COUNTRY_NAME#11,count#12] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/D:/mypro/spark/data-sparkguide/2014-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:int>
== Physical Plan ==
*(2) HashAggregate(keys=[DEST_COUNTRY_NAME#10], functions=[count(1)])
+- Exchange hashpartitioning(DEST_COUNTRY_NAME#10, 5)
   +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#10], functions=[partial_count(1)])
      +- *(1) FileScan csv [DEST_COUNTRY_NAME#10] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/D:/mypro/spark/data-sparkguide/2014-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>
+-----------------+-----------------+
|DEST_COUNTRY_NAME|destination_total|
+-----------------+-----------------+
|    United States|           397187|
|           Canada|             7974|
|           Mexico|             6427|
|   United Kingdom|             1912|
|            Japan|             1591|
+-----------------+-----------------+

+-----------------+-----------------+
|DEST_COUNTRY_NAME|destination_total|
+-----------------+-----------------+
|    United States|           397187|
|           Canada|             7974|
|           Mexico|             6427|
|   United Kingdom|             1912|
|            Japan|             1591|
+-----------------+-----------------+

* */

