package hiveanalyticfunction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HiveAnalyticsFunction {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    //Declaring SparkSession
    val sc = SparkSession
      .builder()
      .appName("Hiveanalyticfunction")
      .master("local")
      .getOrCreate()

    sc.sparkContext.setLogLevel("ERROR")

    val datawinsales=sc.read.
      option("header",true)
      .option("inferschema","true").csv("D:\\mypro\\spark\\winsales.csv")
    datawinsales.show()


    //formatted the dateid to proper date format
    val data= datawinsales.withColumn("dateid1",to_date(col("dateid"),"MM/dd/yyyy"))
        .drop("dateid").withColumnRenamed("dateid1","dateid")
      .selectExpr("SALESID","DATEID","SELLERID","BUYERID","QTY","QTY_SHIPPED")


    data.createOrReplaceTempView("winsales")

    //AVG
    //Compute a rolling average of quantities sold by date; order the results by date ID and sales ID:
    sc.sql("select salesid, dateid, sellerid, qty," +
      "avg(qty) over(order by dateid, salesid rows unbounded preceding) as avg from winsales order by 2,1").show()

    /*COUNT Window Function Examples
      Show the sales ID, quantity, and count of all rows from the beginning of the data window*/

    sc.sql("select salesid, qty," +
      "count(*) over (order by salesid rows unbounded preceding) as count from winsales order by salesid").show()
  }
}
