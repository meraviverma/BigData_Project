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
explode
map
udf
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

    println("#############  Arrays Contains ######################## ")
    df.select(array_contains(split(col("Description")," "),"WHITE")).show(2)

    println("#############  Explode ######################## ")
    df.withColumn("splitted",split(col("Description")," "))
      .withColumn("exploded",explode(col("splitted")))
      .select("Description","InvoiceNo","splitted","exploded").show(2)

    println("#############  Map ######################## ")
     df.select(map(col("Description"),col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['Description']").show(5)

    df.select(map(col("Description"),col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)")
      .show(5)

    println("#############  Working with json ######################## ")
    /*val jsonDF = spark.range(1)
      .selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString
""")
    jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]"
        json_tuple(col("jsonString"), "myJSONKey"))
        .show()*/

     // df.selectExpr("(InvoiceNo,Description) as myStruct").select(to_json(col("myStruct"))).show(5)

    /*val udfExampleDF=*/

    println("#############  Working with UDF ######################## ")
  val udfExampleDF=sc.range(5).toDF("num")
    def power3(number:Double):Double={
      number*number*number
    }
    power3(2.0)

    val power3udf=udf(power3(_:Double):Double)
    udfExampleDF.select(power3udf(col("num"))).show()




  }

}

/*OUTPUT
root
 |-- InvoiceNo: string (nullable = true)
 |-- StockCode: string (nullable = true)
 |-- Description: string (nullable = true)
 |-- Quantity: integer (nullable = true)
 |-- InvoiceDate: timestamp (nullable = true)
 |-- UnitPrice: double (nullable = true)
 |-- CustomerID: double (nullable = true)
 |-- Country: string (nullable = true)

+---+----+---+
|  5|five|5.0|
+---+----+---+
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
|  5|five|5.0|
+---+----+---+
only showing top 20 rows

+---------+-----------------------------------+
|InvoiceNo|Description                        |
+---------+-----------------------------------+
|536365   |WHITE HANGING HEART T-LIGHT HOLDER |
|536365   |WHITE METAL LANTERN                |
|536365   |CREAM CUPID HEARTS COAT HANGER     |
|536365   |KNITTED UNION FLAG HOT WATER BOTTLE|
|536365   |RED WOOLLY HOTTIE WHITE HEART.     |
+---------+-----------------------------------+
only showing top 5 rows

+---------+-----------------------------------+
|InvoiceNo|Description                        |
+---------+-----------------------------------+
|536365   |WHITE HANGING HEART T-LIGHT HOLDER |
|536365   |WHITE METAL LANTERN                |
|536365   |CREAM CUPID HEARTS COAT HANGER     |
|536365   |KNITTED UNION FLAG HOT WATER BOTTLE|
|536365   |RED WOOLLY HOTTIE WHITE HEART.     |
+---------+-----------------------------------+
only showing top 5 rows

#############  Example 2 ######################## +---------+---------+--------------+--------+-------------------+---------+----------+--------------+
|InvoiceNo|StockCode|   Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------+--------+-------------------+---------+----------+--------------+
|   536544|      DOT|DOTCOM POSTAGE|       1|2010-12-01 14:32:00|   569.77|      null|United Kingdom|
|   536592|      DOT|DOTCOM POSTAGE|       1|2010-12-01 17:06:00|   607.49|      null|United Kingdom|
+---------+---------+--------------+--------+-------------------+---------+----------+--------------+

#############  Example 3 ######################## +---------+-----------+
|UnitPrice|isExpensive|
+---------+-----------+
|   569.77|       true|
|   607.49|       true|
+---------+-----------+

+--------------+---------+
|   Description|UnitPrice|
+--------------+---------+
|DOTCOM POSTAGE|   569.77|
|DOTCOM POSTAGE|   607.49|
+--------------+---------+

+---------+---------+-----------+--------+-----------+---------+----------+-------+
|InvoiceNo|StockCode|Description|Quantity|InvoiceDate|UnitPrice|CustomerID|Country|
+---------+---------+-----------+--------+-----------+---------+----------+-------+
+---------+---------+-----------+--------+-----------+---------+----------+-------+

+----------+------------------+
|CustomerId|      realQuantity|
+----------+------------------+
|   17850.0|239.08999999999997|
|   17850.0|          418.7156|
+----------+------------------+
only showing top 2 rows

#############  monotonically_increasing_id() ######################## +-----------------------------+
|monotonically_increasing_id()|
+-----------------------------+
|                            0|
|                            1|
+-----------------------------+
only showing top 2 rows

#############  regexp_replace ########################
+--------------------+--------------------+
|         color_clean|         Description|
+--------------------+--------------------+
|COLOR HANGING HEA...|WHITE HANGING HEA...|
| COLOR METAL LANTERN| WHITE METAL LANTERN|
+--------------------+--------------------+
only showing top 2 rows

#############  translate ########################
+----------------------------------+--------------------+
|translate(Description, LEET, 1337)|         Description|
+----------------------------------+--------------------+
|              WHI73 HANGING H3A...|WHITE HANGING HEA...|
|               WHI73 M37A1 1AN73RN| WHITE METAL LANTERN|
+----------------------------------+--------------------+
only showing top 2 rows

#############  regex_extract ########################
+-----------+----------------------------------+
|color_clean|Description                       |
+-----------+----------------------------------+
|WHITE      |WHITE HANGING HEART T-LIGHT HOLDER|
|WHITE      |WHITE METAL LANTERN               |
+-----------+----------------------------------+
only showing top 2 rows

#############  drop ########################
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|
|   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|
|   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|
|   536367|    22745|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
|   536367|    22748|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
|   536367|    22749|FELTCRAFT PRINCES...|       8|2010-12-01 08:34:00|     3.75|   13047.0|United Kingdom|
|   536367|    22310|IVORY KNITTED MUG...|       6|2010-12-01 08:34:00|     1.65|   13047.0|United Kingdom|
|   536367|    84969|BOX OF 6 ASSORTED...|       6|2010-12-01 08:34:00|     4.25|   13047.0|United Kingdom|
|   536367|    22623|BOX OF VINTAGE JI...|       3|2010-12-01 08:34:00|     4.95|   13047.0|United Kingdom|
|   536367|    22622|BOX OF VINTAGE AL...|       2|2010-12-01 08:34:00|     9.95|   13047.0|United Kingdom|
|   536367|    21754|HOME BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
|   536367|    21755|LOVE BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
|   536367|    21777|RECIPE BOX WITH M...|       4|2010-12-01 08:34:00|     7.95|   13047.0|United Kingdom|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
only showing top 20 rows

#############  fill ########################
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|
|   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|
|   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|
|   536367|    22745|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
|   536367|    22748|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
|   536367|    22749|FELTCRAFT PRINCES...|       8|2010-12-01 08:34:00|     3.75|   13047.0|United Kingdom|
|   536367|    22310|IVORY KNITTED MUG...|       6|2010-12-01 08:34:00|     1.65|   13047.0|United Kingdom|
|   536367|    84969|BOX OF 6 ASSORTED...|       6|2010-12-01 08:34:00|     4.25|   13047.0|United Kingdom|
|   536367|    22623|BOX OF VINTAGE JI...|       3|2010-12-01 08:34:00|     4.95|   13047.0|United Kingdom|
|   536367|    22622|BOX OF VINTAGE AL...|       2|2010-12-01 08:34:00|     9.95|   13047.0|United Kingdom|
|   536367|    21754|HOME BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
|   536367|    21755|LOVE BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
|   536367|    21777|RECIPE BOX WITH M...|       4|2010-12-01 08:34:00|     7.95|   13047.0|United Kingdom|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
only showing top 20 rows

#############  replace ########################
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|
|   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|
|   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|
|   536367|    22745|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
|   536367|    22748|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
|   536367|    22749|FELTCRAFT PRINCES...|       8|2010-12-01 08:34:00|     3.75|   13047.0|United Kingdom|
|   536367|    22310|IVORY KNITTED MUG...|       6|2010-12-01 08:34:00|     1.65|   13047.0|United Kingdom|
|   536367|    84969|BOX OF 6 ASSORTED...|       6|2010-12-01 08:34:00|     4.25|   13047.0|United Kingdom|
|   536367|    22623|BOX OF VINTAGE JI...|       3|2010-12-01 08:34:00|     4.95|   13047.0|United Kingdom|
|   536367|    22622|BOX OF VINTAGE AL...|       2|2010-12-01 08:34:00|     9.95|   13047.0|United Kingdom|
|   536367|    21754|HOME BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
|   536367|    21755|LOVE BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
|   536367|    21777|RECIPE BOX WITH M...|       4|2010-12-01 08:34:00|     7.95|   13047.0|United Kingdom|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
only showing top 20 rows

#############  working with complex Types ########################
root
 |-- complex: struct (nullable = false)
 |    |-- Description: string (nullable = true)
 |    |-- InvoiceNo: string (nullable = true)

+-----------------------------------+
|complex.Description                |
+-----------------------------------+
|WHITE HANGING HEART T-LIGHT HOLDER |
|WHITE METAL LANTERN                |
|CREAM CUPID HEARTS COAT HANGER     |
|KNITTED UNION FLAG HOT WATER BOTTLE|
|RED WOOLLY HOTTIE WHITE HEART.     |
+-----------------------------------+
only showing top 5 rows

+--------------------+---------+
|         Description|InvoiceNo|
+--------------------+---------+
|WHITE HANGING HEA...|   536365|
| WHITE METAL LANTERN|   536365|
|CREAM CUPID HEART...|   536365|
|KNITTED UNION FLA...|   536365|
|RED WOOLLY HOTTIE...|   536365|
+--------------------+---------+
only showing top 5 rows

#############  working with Arrays Types ########################
+----------------------------------------+
|split(Description,  )                   |
+----------------------------------------+
|[WHITE, HANGING, HEART, T-LIGHT, HOLDER]|
|[WHITE, METAL, LANTERN]                 |
+----------------------------------------+
only showing top 2 rows

+------------+
|array_col[0]|
+------------+
|WHITE       |
|WHITE       |
+------------+
only showing top 2 rows

+---------------------------+
|size(split(Description,  ))|
+---------------------------+
|                          5|
|                          3|
+---------------------------+
only showing top 2 rows

#############  Arrays Contains ########################
+--------------------------------------------+
|array_contains(split(Description,  ), WHITE)|
+--------------------------------------------+
|                                        true|
|                                        true|
+--------------------------------------------+
only showing top 2 rows

#############  Explode ########################
+--------------------+---------+--------------------+--------+
|         Description|InvoiceNo|            splitted|exploded|
+--------------------+---------+--------------------+--------+
|WHITE HANGING HEA...|   536365|[WHITE, HANGING, ...|   WHITE|
|WHITE HANGING HEA...|   536365|[WHITE, HANGING, ...| HANGING|
+--------------------+---------+--------------------+--------+
only showing top 2 rows

#############  Map ########################
+------------------------+
|complex_map[Description]|
+------------------------+
|                    null|
|                    null|
|                    null|
|                    null|
|                    null|
+------------------------+
only showing top 5 rows

+--------------------+------+
|                 key| value|
+--------------------+------+
|WHITE HANGING HEA...|536365|
| WHITE METAL LANTERN|536365|
|CREAM CUPID HEART...|536365|
|KNITTED UNION FLA...|536365|
|RED WOOLLY HOTTIE...|536365|
+--------------------+------+
only showing top 5 rows

#############  Working with json ########################
+--------+
|UDF(num)|
+--------+
|     0.0|
|     1.0|
|     8.0|
|    27.0|
|    64.0|
+--------+



 */