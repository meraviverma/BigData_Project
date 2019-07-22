package DemoFileFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DemoJson1 {
  def main(args: Array[String]):Unit= {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    //Declaring SparkSession
    val sc = SparkSession
      .builder()
      .appName("")
      .master("local")
      .getOrCreate()

    sc.sparkContext.setLogLevel("ERROR")


    val df=sc.read.option("multiline","true")json("D:\\mypro\\spark\\sample.json")

    df.show(truncate = false)

    println(df.printSchema())

    val dfDates=df.select(explode(df("dates"))).toDF("Dates")

    dfDates.show()

    val dfContent=df.select(explode(df("content"))).toDF("content")
    dfContent.show()

    //extracting fields in struct
    val dfFooBar=dfContent.select("content.foo","content.bar")
    //dfFooBar.show()

  }

}
