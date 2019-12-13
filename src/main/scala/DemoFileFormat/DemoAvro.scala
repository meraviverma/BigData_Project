package DemoFileFormat

import org.apache.spark.sql.SparkSession

object DemoAvro {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    //Declaring SparkSession
    val sc = SparkSession
      .builder()
      .appName("Avro Data")
      .master("local")
      .getOrCreate()

    sc.sparkContext.setLogLevel("ERROR")

    val dataavro=sc.read.format("com.databricks.spark.avro").load("D:\\mypro\\spark\\part-00000-avro-data.avro")
    dataavro.printSchema()

    dataavro.show()

    val datapar=dataavro
  }
}
