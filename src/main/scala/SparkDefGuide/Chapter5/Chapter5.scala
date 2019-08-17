package SparkDefGuide.Chapter5

import conf.SparkConfiguration
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{expr,col,column,lit,desc,asc}
/*
read.format("json")
printschema
schema
StructType
StructField
We can take advantage of toDF on a Seq type
Different ways to select columns in spark(col,column,expr)
selectExpr
Literal - lit: used to add a constant value
withColumn
withColumnRenamed
drop
changing column type(cast)
filter
orderBy
sort
repartition
DataFrame is just a Dataset of Rows types
 */
object Chapter5 extends SparkConfiguration {
  def main(args: Array[String]):Unit={

    import sc.implicits._

    val df=sc.read.format("json").load("D:\\mypro\\spark\\data-sparkguide\\2015-summary.json")
    df.printSchema()

    sc.read.format("json").load("D:\\mypro\\spark\\data-sparkguide\\2015-summary.json").schema

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME",StringType,true),
      StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      StructField("count",LongType,false,
        Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df1=sc.read.format("json").schema(myManualSchema).load("D:\\mypro\\spark\\data-sparkguide\\2015-summary.json")
    df1.printSchema()

    //comparing tow schema
    val schemaUntyped = new StructType()
      .add("a", "int")
      .add("b", "string")
    val schemaTyped = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)

    println(schemaUntyped == schemaTyped)

    //We can also create DataFrame on the fly by taking a set of rows and converting them to a dataframe.

    println("################# Creating DataFrame ############################3")
    val myManualSchema1=new StructType(Array(
      new StructField("some",StringType,true),
      new StructField("col",StringType,true),
      new StructField("names",LongType,false)
    ))
    val myRows=Seq(Row("Hello",null,1L))
    val myrdd=sc.sparkContext.parallelize(myRows)
    val myDf=sc.createDataFrame(myrdd,myManualSchema1)
    myDf.show()

    val myDf1=Seq(("Hello",2,1L)).toDF("col1","col2","col3")
    myDf1.show()

    println("########### Different Ways to perform Select ##################")
    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      'DEST_COUNTRY_NAME,
      $"DEST_COUNTRY_NAME",
      expr("DEST_COUNTRY_NAME"),
      expr("DEST_COUNTRY_NAME as destination"),
      expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_name")
    ).show(2)

    df.selectExpr("*","(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)

    df.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show(3)

    println("######### converting to spark Types (Literal) ############")
    df.select(expr("*"),lit(1).as("one")).show(2)

    println("################ Adding Column #################")
    df.withColumn("number one",lit(1)).show(2)

    //withColumn takes two argument: the column name and the expression that will create the value for that given row
    df.withColumn("withinCountry",expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME")).show(2)

    println("################## Renaming a column ###################")

    df.withColumnRenamed("DEST_COUNTRY_NAME","dest").show(2)

    println("################## changing a column Type ###################")
    df.withColumn("count2",col("count").cast("long")).show(2)

    println("################## Filtering  rows ###################")
    df.filter(col("count") < 2).show(2)

    df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Coratia").show(2)

    println("################## Getting Unique  rows ###################")
    df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct().count()
    df.select("ORIGIN_COUNTRY_NAME").distinct().count()

    println("################## Concatenating and appending Rows  rows ###################")
    val schema=df.schema
    val newRows=Seq(
      Row("New Country","Other Country",5L),
      Row("New country 2","Other Country 3",1L)
    )

    val parallelizedRows=sc.sparkContext.parallelize(newRows)
    val newDF=sc.createDataFrame(parallelizedRows,schema)
    df.union(newDF)
      .where("count=1")
      .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
      .show()

    println("################## Sorting Rows ###################")
    df.sort("count").show(5)
    df.orderBy("count","DEST_COUNTRY_NAME").show(5)
    df.orderBy(col("count"),col("DEST_COUNTRY_NAME")).show(5)

    df.orderBy(expr("count desc")).show(2)
    df.orderBy(desc("count"),asc("DEST_COUNTRY_NAME")).show(5)

    df.limit(5).show()

    println("################## Repartition and coalesce ###################")
    df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)
    //This operation will shuffle your data into five partition based on the detination country name and then coalesce them
    //(without the full shuffle)

  }

}

//OUTPUT
/*
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: long (nullable = true)

root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: long (nullable = true)

true
################# Creating DataFrame ############################3
+-----+----+-----+
| some| col|names|
+-----+----+-----+
|Hello|null|    1|
+-----+----+-----+

+-----+----+----+
| col1|col2|col3|
+-----+----+----+
|Hello|   2|   1|
+-----+----+----+

########### Different Ways to perform Select ##################
+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-------------+-----------------+
|DEST_COUNTRY_NAME|DEST_COUNTRY_NAME|DEST_COUNTRY_NAME|DEST_COUNTRY_NAME|DEST_COUNTRY_NAME|DEST_COUNTRY_NAME|  destination|DEST_COUNTRY_name|
+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-------------+-----------------+
|    United States|    United States|    United States|    United States|    United States|    United States|United States|    United States|
|    United States|    United States|    United States|    United States|    United States|    United States|United States|    United States|
+-----------------+-----------------+-----------------+-----------------+-----------------+-----------------+-------------+-----------------+
only showing top 2 rows

+-----------------+-------------------+-----+-------------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|withinCountry|
+-----------------+-------------------+-----+-------------+
|    United States|            Romania|   15|        false|
|    United States|            Croatia|    1|        false|
+-----------------+-------------------+-----+-------------+
only showing top 2 rows

+-----------+---------------------------------+
| avg(count)|count(DISTINCT DEST_COUNTRY_NAME)|
+-----------+---------------------------------+
|1770.765625|                              132|
+-----------+---------------------------------+

######### converting to spark Types (Literal) ############
+-----------------+-------------------+-----+---+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|one|
+-----------------+-------------------+-----+---+
|    United States|            Romania|   15|  1|
|    United States|            Croatia|    1|  1|
+-----------------+-------------------+-----+---+
only showing top 2 rows

################ Adding Column #################
+-----------------+-------------------+-----+----------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|number one|
+-----------------+-------------------+-----+----------+
|    United States|            Romania|   15|         1|
|    United States|            Croatia|    1|         1|
+-----------------+-------------------+-----+----------+
only showing top 2 rows

+-----------------+-------------------+-----+-------------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|withinCountry|
+-----------------+-------------------+-----+-------------+
|    United States|            Romania|   15|        false|
|    United States|            Croatia|    1|        false|
+-----------------+-------------------+-----+-------------+
only showing top 2 rows

################## Renaming a column ###################
+-------------+-------------------+-----+
|         dest|ORIGIN_COUNTRY_NAME|count|
+-------------+-------------------+-----+
|United States|            Romania|   15|
|United States|            Croatia|    1|
+-------------+-------------------+-----+
only showing top 2 rows

################## changing a column Type ###################
+-----------------+-------------------+-----+------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|count2|
+-----------------+-------------------+-----+------+
|    United States|            Romania|   15|    15|
|    United States|            Croatia|    1|     1|
+-----------------+-------------------+-----+------+
only showing top 2 rows

################## Filtering  rows ###################
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Croatia|    1|
|    United States|          Singapore|    1|
+-----------------+-------------------+-----+
only showing top 2 rows

+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Croatia|    1|
|    United States|          Singapore|    1|
+-----------------+-------------------+-----+
only showing top 2 rows

################## Getting Unique  rows ###################
################## Concatenating and appending Rows  rows ###################
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Croatia|    1|
|    United States|          Singapore|    1|
|    United States|          Gibraltar|    1|
|    United States|             Cyprus|    1|
|    United States|            Estonia|    1|
|    United States|          Lithuania|    1|
|    United States|           Bulgaria|    1|
|    United States|            Georgia|    1|
|    United States|            Bahrain|    1|
|    United States|   Papua New Guinea|    1|
|    United States|         Montenegro|    1|
|    United States|            Namibia|    1|
|    New country 2|    Other Country 3|    1|
+-----------------+-------------------+-----+

################## Sorting Rows ###################
+--------------------+-------------------+-----+
|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+--------------------+-------------------+-----+
|               Malta|      United States|    1|
|Saint Vincent and...|      United States|    1|
|       United States|            Croatia|    1|
|       United States|          Gibraltar|    1|
|       United States|          Singapore|    1|
+--------------------+-------------------+-----+
only showing top 5 rows

+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|     Burkina Faso|      United States|    1|
|    Cote d'Ivoire|      United States|    1|
|           Cyprus|      United States|    1|
|         Djibouti|      United States|    1|
|        Indonesia|      United States|    1|
+-----------------+-------------------+-----+
only showing top 5 rows

+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|     Burkina Faso|      United States|    1|
|    Cote d'Ivoire|      United States|    1|
|           Cyprus|      United States|    1|
|         Djibouti|      United States|    1|
|        Indonesia|      United States|    1|
+-----------------+-------------------+-----+
only showing top 5 rows

+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|          Moldova|      United States|    1|
|    United States|            Croatia|    1|
+-----------------+-------------------+-----+
only showing top 2 rows

+-----------------+-------------------+------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
+-----------------+-------------------+------+
|    United States|      United States|370002|
|    United States|             Canada|  8483|
|           Canada|      United States|  8399|
|    United States|             Mexico|  7187|
|           Mexico|      United States|  7140|
+-----------------+-------------------+------+
only showing top 5 rows

+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|   15|
|    United States|            Croatia|    1|
|    United States|            Ireland|  344|
|            Egypt|      United States|   15|
|    United States|              India|   62|
+-----------------+-------------------+-----+

################## Repartition and coalesce ###################
 */
