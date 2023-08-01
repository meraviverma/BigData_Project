package src.main.scala.DeltaTableExample

import org.apache.spark.sql.SparkSession

object DeltaTableExample {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "E:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("DeltaTableExample")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.range(0, 5)
    //data.write.format("delta").save("D:\\SparkProject\\data-sparkguide\\delta-table")

    val df = spark.read.format("delta").load("D:\\SparkProject\\data-sparkguide\\delta-table")
    df.show()

    //val df1 = spark.read.format("delta").options("timestampAsOf", timestamp_string).load("/tmp/delta/people10m")
    //val df2 = spark.read.format("delta").options("versionAsOf", version).load("/tmp/delta/people10m")



  }


}
