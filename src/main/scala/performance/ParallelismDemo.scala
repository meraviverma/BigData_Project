package src.main.scala.performance

import org.apache.spark.sql.SparkSession

object ParallelismDemo extends App {
  System.setProperty("hadoop.home.dir", "E:\\software\\winutils-master\\hadoop")
  val sc = SparkSession
    .builder()
    .appName(" first example")
    .master("local[2]")
    .getOrCreate()


  sc.sparkContext.setLogLevel("ERROR")
  println("Hello")
  //sc.sparkContext.set("spark.sql.adaptive.enabled", False)
  //spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", False)
  //spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
  println(sc.conf.get("spark.sql.shuffle.partitions")) //200 when set to local

  println(sc.sparkContext.defaultParallelism) //1 //1 when set to local
  //2 when set to local[2]

  



}
