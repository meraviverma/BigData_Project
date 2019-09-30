package SparkDefGuide.Chapter7

import conf.SparkConfiguration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/*
count
countDistinct
approx_count_distinct
first
last
min
max
sum
sumDistinct
avg
var_pop("Quantity"),
var_samp("Quantity"),
stddev_pop("Quantity"),
stddev_samp("Quantity"))
skewness("Quantity"),
kurtosis("Quantity"))
corr("InvoiceNo", "Quantity"),
covar_samp("InvoiceNo", "Quantity"),
covar_pop("InvoiceNo", "Quantity"))
collect_set("Country"),
collect_list("Country"))
groupBy
df.groupBy("InvoiceNo")
.agg(
count("Quantity").alias("quan"),
expr("count(Quantity)"))
.show()
-------------------
grouping With maps
--------------------
df.groupBy("InvoiceNo")
.agg(
"Quantity" ->"avg",
"Quantity" -> "stddev_pop")
.show()

--------------------------
Window Functions
---------------------------
Rollups
Cube
grouping_id()
Pivot
 */
object Chapter7 extends  SparkConfiguration{
  def main(args: Array[String]): Unit = {

    val df=sc.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\rv00451128\\Desktop\\tutorial\\dataset\\Spark-The-Definitive-Guide-master\\Spark-The-Definitive-Guide-master\\data\\retail-data\\all\\*.csv")
      .coalesce(5)

    df.cache()

    df.createOrReplaceTempView("dfTable")

    println("############## COUNT #################")
    df.select(count("StockCode")).show(10)

    println("############## COUNT DISTINCT #################")
    df.select(countDistinct("StockCode")).show()

    println("############## Approx COUNT DISTINCT #################")
    df.select(approx_count_distinct("StockCode",0.1)).show()

    println("############## Grouping with Maps #################")
    df.groupBy("InvoiceNo")
      .agg(
        "Quantity" -> "avg",
        "Quantity" ->"stddev_pop"
      ).show()


    println("############## Window Functions #################")
    /*Window function allows to perform some unique aggregations on a specific window of
    data where we define the window with reference to the current data.
    Three kind of window ranking function, analytics function, aggregate functions
    This window specification determines which rows will be passed into this function*/

    val dfWithDate= df.withColumn("date", to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")

    val windowSpec=Window
      .partitionBy("CustomerId","date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    //rowsBetween states which rows will be included in the frame based on its reference to the current row
    val maxPurchaseQuantity=max(col("Quantity"))
      .over(windowSpec)
    //maxPurchaseQuantity


    val purchaseDenseRank=rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    dfWithDate.where("CustomerID IS NOT NULL").orderBy("CustomerId").orderBy("CustomerID")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")).show(5)



    /*
    ############## COUNT #################
+----------------+
|count(StockCode)|
+----------------+
|          541909|
+----------------+

############## COUNT DISTINCT #################
+-------------------------+
|count(DISTINCT StockCode)|
+-------------------------+
|                     4070|
+-------------------------+

############## Approx COUNT DISTINCT #################
+--------------------------------+
|approx_count_distinct(StockCode)|
+--------------------------------+
|                            3364|
+--------------------------------+

############## Grouping with Maps #################
+---------+------------------+--------------------+
|InvoiceNo|     avg(Quantity)|stddev_pop(Quantity)|
+---------+------------------+--------------------+
|   536596|               1.5|  1.1180339887498947|
|   536938|33.142857142857146|  20.698023172885524|
|   537252|              31.0|                 0.0|
|   537691|              8.15|   5.597097462078001|
|   538041|              30.0|                 0.0|
|   538184|12.076923076923077|   8.142590198943392|
|   538517|3.0377358490566038|  2.3946659604837897|
|   538879|21.157894736842106|  11.811070444356483|
|   539275|              26.0|  12.806248474865697|
|   539630|20.333333333333332|  10.225241100118645|
|   540499|              3.75|  2.6653642652865788|
|   540540|2.1363636363636362|  1.0572457590557278|
|  C540850|              -1.0|                 0.0|
|   540976|10.520833333333334|   6.496760677872902|
|   541432|             12.25|  10.825317547305483|
|   541518| 23.10891089108911|  20.550782784878713|
|   541783|11.314285714285715|   8.467657556242811|
|   542026| 7.666666666666667|   4.853406592853679|
|   542375|               8.0|  3.4641016151377544|
|  C542604|              -8.0|  15.173990905493518|
+---------+------------------+--------------------+
only showing top 20 rows

############## Window Functions #################
+----------+----------+--------+------------+-----------------+-------------------+
|CustomerId|      date|Quantity|quantityRank|quantityDenseRank|maxPurchaseQuantity|
+----------+----------+--------+------------+-----------------+-------------------+
|     12346|2011-01-18|   74215|           1|                1|              74215|
|     12346|2011-01-18|  -74215|           2|                2|              74215|
|     12347|2010-12-07|      36|           1|                1|                 36|
|     12347|2010-12-07|      30|           2|                2|                 36|
|     12347|2010-12-07|      24|           3|                3|                 36|
+----------+----------+--------+------------+-----------------+-------------------+
only showing top 5 rows
     */





  }

}
