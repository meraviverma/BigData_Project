package DemoFileFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//###################################
//This will load data from Json and make dataframe
object DemoJson {
  def main(args: Array[String]):Unit={

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    //Declaring SparkSession
    val sc=SparkSession
      .builder()
      .appName("")
      .master("local")
      .getOrCreate()

    sc.sparkContext.setLogLevel("ERROR")
    import sc.implicits._

    /*Error Occurred 1:
      root
    |-- _corrupt_record: string (nullable = true)

    ()
    Multi-line mode
    If a JSON object occupies multiple lines, you must enable multi-line mode for Spark to load the file.
    Files will be loaded as a whole entity and cannot be split.
    Use read.option("multiline","true")
    */

    val student_detail =sc.read.option("multiline","true").json("D:\\mypro\\spark\\student_detai.json")


    //println(student_detail.printSchema())

    //Nested Json
    val student: DataFrame =sc.read.option("multiline","true").json("D:\\mypro\\spark\\student.json")
    println(student.printSchema())

    val add=student.withColumn("address",explode(array("address"))).select("address.city","address.state").show(truncate = false)


    student.select(explode(student("phoneNumbers"))).show

    student.createOrReplaceTempView("student")
    sc.sql("select explode(phoneNumbers) from student").show()


  }

}
