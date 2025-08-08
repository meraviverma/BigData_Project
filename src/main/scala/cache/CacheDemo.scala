package src.main.scala.cache

import conf.SparkConfiguration
import org.apache.spark.sql.functions.expr

import java.lang.Thread.sleep

object CacheDemo extends SparkConfiguration {
  def main(args: Array[String]):Unit= {

    val my_df=sc.range(1,1000000).toDF("id")
      .repartition(10)
      .withColumn("square",expr("id*1d"))
      .cache()

    //my_df.take(10)

    my_df.count()

    sleep(50000000)
  }

}
