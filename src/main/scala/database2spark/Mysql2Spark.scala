package database2spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object Mysql2Spark {
  def main(args: Array[String]):Unit= {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    //Declaring SparkSession
    val sc = SparkSession
      .builder()
      .appName("")
      .master("local")
      .getOrCreate()

    sc.sparkContext.setLogLevel("ERROR")
    import sc.implicits._

    //add database input options separately
    /*val df = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/data").
      option("dbtable", "cus_tbl").
      option("driver", "com.mysql.jdbc.Driver").
      option("user", "root").
      option("password", "root").
      load();*/

    //read data from mysql table

    //add database input options as Map
    val df: DataFrame = sc.read.format("jdbc").
      options(Map(
        "url" -> "jdbc:mysql://localhost:3306/mydb",
        "dbtable" -> "cus_tbl",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "password" -> "root")).
      load();

    df.printSchema()
    df.show()

    ///////////// ####### WRITE DATA TO MYSQL TABLE FROM SPARK DATAFRAME ###### ///////////////////
    //create properties object
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    //jdbc mysql url - destination database is named "data"
    val url = "jdbc:mysql://localhost:3306/mydb"

    //destination database table
    val table = "cus_tb2"

    //write data from spark dataframe to database
    df.write.mode("append").jdbc(url, table, prop)
  }

  /////////// OUTPUT ///////////
  /*root
  |-- cus_id: integer (nullable = true)
  |-- cus_firstname: string (nullable = true)
  |-- cus_surname: string (nullable = true)

  +------+-------------+-----------+
  |cus_id|cus_firstname|cus_surname|
  +------+-------------+-----------+
  |     5|        Ajeet|     Maurya|
  |     6|      Deepika|     Chopra|
  |     7|        Vimal|    Jaiswal|
  +------+-------------+-----------+*/



}
