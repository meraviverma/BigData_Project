package src.main.scala.repartitioncoalesce

import conf.SparkConfiguration

import java.lang.Thread.sleep

object repartitionDemo extends SparkConfiguration {

  def main(args: Array[String]):Unit= {

    val my_df = sc.range(1, 1000000).toDF("id")
      .repartition(10)
      .cache()

    my_df.count()

    //my_df.repartition("id")

    sleep(50000000)

  }

}
