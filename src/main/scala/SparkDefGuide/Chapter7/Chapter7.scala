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
grouping With maps
--------------------
df.groupBy("InvoiceNo")
.agg(
"Quantity" ->"avg",
"Quantity" -> "stddev_pop")
.show()


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

    val dfWithDate= df.withColumn("date", col("InvoiceData"))
    dfWithDate.createOrReplaceTempView("dfWithDate")

    val windowSpec=Window
      .partitionBy("CustomerId","date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    //rowsBetween states which rows will be included in the frame based on its reference to the current row
    val maxPurchaseQuantity=max(col("Quantity"))
      .over(windowSpec)
    //maxPurchaseQuantity



  }

}
