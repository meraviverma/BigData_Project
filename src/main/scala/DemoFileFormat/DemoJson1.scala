package DemoFileFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//http://bigdatums.net/2016/02/12/how-to-extract-nested-json-data-in-spark/
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
    dfFooBar.show()

  }

}
/* OUTPUT
+-----------------------------------------------------------------+------------------------+-----------+------+-----------+
|content                                                          |dates                   |reason     |status|user       |
+-----------------------------------------------------------------+------------------------+-----------+------+-----------+
|[[val1, 123], [val2, 456], [val3, 789], [val4, 124], [val5, 126]]|[2016-01-29, 2016-01-28]|some reason|OK    |gT35Hhhre9m|
+-----------------------------------------------------------------+------------------------+-----------+------+-----------+

root
 |-- content: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- bar: string (nullable = true)
 |    |    |-- foo: long (nullable = true)
 |-- dates: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- reason: string (nullable = true)
 |-- status: string (nullable = true)
 |-- user: string (nullable = true)

()
+----------+
|     Dates|
+----------+
|2016-01-29|
|2016-01-28|
+----------+

+-----------+
|    content|
+-----------+
|[val1, 123]|
|[val2, 456]|
|[val3, 789]|
|[val4, 124]|
|[val5, 126]|
+-----------+

+---+----+
|foo| bar|
+---+----+
|123|val1|
|456|val2|
|789|val3|
|124|val4|
|126|val5|
+---+----+

* */