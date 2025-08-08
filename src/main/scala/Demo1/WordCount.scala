package Demo1

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("first example")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //Rdd
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3));
    val nums = rdd.collect()
    val result = nums.mkString(",")
    println(result)

    println(rdd.partitions.size)




  }
}
