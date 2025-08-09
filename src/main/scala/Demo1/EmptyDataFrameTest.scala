package src.main.scala.Demo1
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object EmptyDataFrameTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("first example")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val  df= spark.emptyDataFrame
    df.show()
    }
}
