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


    println(student_detail.printSchema())

    //Nested Json
    val student: DataFrame =sc.read.option("multiline","true").json("D:\\mypro\\spark\\student.json")
    println(student.printSchema())

    val add=student.withColumn("address",explode(array("address"))).select("address.city","address.state")
    add.show()


   /* Flattening structs
      A star (*) can be used to select all of the subfields in a struct.*/

    var dfPhoneNumber=student.select(explode(student("phoneNumbers"))).toDF("phonenumber")
    var dfNumber=dfPhoneNumber.select("phonenumber.number","phonenumber.type")
    var dfNumber1=dfPhoneNumber.select("phonenumber.*")

    dfNumber.show()
    dfNumber1.show()

    student.createOrReplaceTempView("student")
    sc.sql("select explode(phoneNumbers) from student").show()

    /* OUTPUT

root
 |-- Accessory: string (nullable = true)
 |-- BreastSize: string (nullable = true)
 |-- Class: string (nullable = true)
 |-- Club: string (nullable = true)
 |-- Color: string (nullable = true)
 |-- Crush: string (nullable = true)
 |-- EyeType: string (nullable = true)
 |-- Eyes: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Hairstyle: string (nullable = true)
 |-- ID: string (nullable = true)
 |-- Info: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Persona: string (nullable = true)
 |-- ScheduleAction: string (nullable = true)
 |-- ScheduleDestination: string (nullable = true)
 |-- ScheduleTime: string (nullable = true)
 |-- Seat: string (nullable = true)
 |-- Stockings: string (nullable = true)
 |-- Strength: string (nullable = true)

()
root
 |-- address: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- postalCode: string (nullable = true)
 |    |-- state: string (nullable = true)
 |    |-- streetAddress: string (nullable = true)
 |-- age: long (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- phoneNumbers: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- number: string (nullable = true)
 |    |    |-- type: string (nullable = true)

()
+-----+-----+
| city|state|
+-----+-----+
|Surat|   GJ|
+-----+-----+

+----------+----+
|    number|type|
+----------+----+
|7383627627|home|
+----------+----+

+----------+----+
|    number|type|
+----------+----+
|7383627627|home|
+----------+----+

+------------------+
|               col|
+------------------+
|[7383627627, home]|
+------------------+


    * */

  }

}
