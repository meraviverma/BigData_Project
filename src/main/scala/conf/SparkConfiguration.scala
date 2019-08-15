package conf

import org.apache.spark.sql.SparkSession

class SparkConfiguration {
  System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
  val sc = SparkSession
    .builder()
    .appName(" first example")
    .master("local")
    .getOrCreate()


  sc.sparkContext.setLogLevel("ERROR")

  import sc.implicits._

}
