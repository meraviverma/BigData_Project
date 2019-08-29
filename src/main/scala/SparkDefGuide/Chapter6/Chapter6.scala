package SparkDefGuide.Chapter6

import conf.SparkConfiguration
import org.apache.spark.sql.functions._

/*
lit function - converts a type in another language to its spark representation.
In Spark for equality we use === (equal) or =!= (not equal)
eqNullSafe
pow
round
bround
corr( correlation coefficient)
count
mean
stddev_pop
min
max
monotonically_increasing_id
nanvl : Returns col1 if it is not NaN, or col2 if col1 is NaN.
initcap
lower, upper, ltrim, rtrim, rpad, lpad , trim
regexp_extract
regexp_replace
instr

Working with dates and timestamps:
-----------------------------------
current_date
current_timestamp
date_sub
date_add
datediff
months_between
to_date
lit
to_date
to_timestamp
Working With Null:
-------------------
Coalesce
ifnull
nullif
nvl
nvl2
drop
fill
asc_nulls_first
desc_nulls_first
asc_nulls_last
desc_nulls_last

working with complex Types
Structs
Arrays
array_contains
size
 */
object Chapter6 extends  SparkConfiguration{

  def main(args: Array[String]): Unit = {
    val df=sc.read.format("csv")
      .option("header",true)
      .option("inferschema","true")
      .load("D:\\mypro\\spark\\data-sparkguide\\2010-12-01.csv")

    df.printSchema()
    df.createOrReplaceTempView("dfTable")

    //CONVERTING TO SPARK TYPES
    df.select(lit(5),lit("five"),lit(5.0)).show()

    //WORKING WITH BOOLEANS
    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo","Description")
      .show(5,false)

    df.where(col("InvoiceNo") === (536365))
      .select("InvoiceNo","Description")
      .show(5,false)

    print("#############  Example 2 ######################## ")
    val priceFilter=col("UnitPrice") > 600
    val descripFilter=col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).show()

    print("#############  Example 3 ######################## ")
    val DOTCodeFilter=col("StockCode") === "DOT"
    df.withColumn("isExpensive",DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("UnitPrice","isExpensive").show(5)

    df.withColumn("isExpensive",not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description","UnitPrice").show(5)

    //NOTE: When working with null data, If there is null in your data, we need to perform null-safe equivalent test.

    df.where(col("Description").eqNullSafe("hello")).show()

    val fabricatedQuantity=pow(col("Quantity") * col("UnitPrice"),2) + 5
    df.select(expr("CustomerId"),fabricatedQuantity.alias("realQuantity")).show(2)

    print("#############  monotonically_increasing_id() ######################## ")
    df.select(monotonically_increasing_id()).show(2)

    println("#############  regexp_replace ######################## ")
    val simpleColors=Seq("black","white","red","green","blue")
    val regexString=simpleColors.map(_.toUpperCase()).mkString("|")

    df.select(regexp_replace(col(("Description")),regexString,"COLOR").alias("color_clean")
      ,col("Description")).show(2)

    println("#############  translate ######################## ")
    df.select(translate(col("Description"),"LEET","1337"),col("Description")).show(2)

    println("#############  regex_extract ######################## ")
    val regexString1=simpleColors.map(_.toUpperCase()).mkString("(","|",")")
    df.select(
      regexp_extract(col("Description"),regexString1,1).alias("color_clean"),
      col("Description")).show(2,false)

    println("#############  drop ######################## ")
    df.na.drop("any")
    df.na.drop("all",Seq("StockCode","InvoiceNo")).show()
    //Specifying "any" as an argument drops a row if any of the values are null.
    //Using "all" drops the row if all the values are null or NaN for that row.

    println("#############  fill ######################## ")
    //Using the fill you can fill one or more column with a set of values.
    //This can be done by specifying a map
    df.na.fill("All Null values become this string")
    df.na.fill(5,Seq("StockCode","InvoiceNo"))
    df.na.fill("all",Seq("StockCode","InvoiceNo"))

    val fillColValues=Map("StockCode"->5,"Description"->"No Value")
    df.na.fill(fillColValues).show()

    println("#############  replace ######################## ")
    //Replace all values in a certain column according to there current value
    //Only requirement is that this value be the same type as the original value
    df.na.replace("Description",Map(""->"UNKNOWN")).show()

    println("#############  working with complex Types ######################## ")

    df.selectExpr("(Description,InvoiceNo) as complex","*")
    df.selectExpr("struct(Description,InvoiceNo) as complex","*")

    val complexDf=df.select(struct("Description","InvoiceNo").alias("complex"))
    complexDf.printSchema()
    complexDf.createOrReplaceTempView("complexDf")

    complexDf.select("complex.Description")
    complexDf.select(col("complex").getField("Description")).show(5,false)
    complexDf.select("complex.*").show(5)

    println("#############  working with Arrays Types ######################## ")
    df.select(split(col("Description")," ")).show(2,false)

    df.select(split(col(("Description"))," ").alias("array_col"))
      .selectExpr("array_col[0]").show(2,false)

    df.select(size(split(col("Description")," "))).show(2)

    df.select(array_contains(split(col("Description")," "),"WHITE")).show(2)

  df.withColumn("splitted",split(col("Description")," "))
      .withColumn("exploded",explode(col("splitted")))
      .select("Description","InvoiceNo","splitted","exploded").show(2)
  }

}
