package src.main.scala.DeltaTableExample

import org.apache.spark.sql.SparkSession
import io.delta.tables._

object OptimizeExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("OptimizeExample")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df=spark.range(0,5)

    //df.repartition(5).write.format("delta").save("D:\\SparkProject\\data-sparkguide\\optimize")

    val delta_table=DeltaTable.forPath(spark,"D:\\SparkProject\\data-sparkguide\\optimize")
    delta_table.optimize().executeCompaction()
  }
}
