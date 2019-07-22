package Demo1

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName(" first example")
      .master("local")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //Rdd
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3));
    println(rdd.collect().mkString(","));
    println(rdd.partitions.size)


  }
}
