package src.main.scala.Demo1
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object WordCountAnalytics {

  def tokenizeWords(df: DataFrame): DataFrame = {
    df.withColumn("sentence", lower(col("sentence")))
      .withColumn("word", explode(split(col("sentence"), "\\s+")))
      .filter(col("word") =!= "")
  }

  def overallWordCount(tokenizedDf: DataFrame): DataFrame = {
    tokenizedDf.groupBy("word")
      .agg(count("word").alias("count"))
      .orderBy(desc("count"))
  }

  def topFrequentWords(overallWordCountDf: DataFrame, topN: Int): DataFrame = {
    overallWordCountDf
      .orderBy(desc("count"))
      .limit(topN)
  }

  def sentenceWiseWordCount(df: DataFrame): DataFrame = {
    df.withColumn("sentence", lower(col("sentence")))
      .withColumn("word_count", size(split(col("sentence"), "\\s+")))
      .select(col("id"), col("word_count"))
  }

  def uniqueWordCount(tokenizedDf: DataFrame): Long = {
    tokenizedDf.select("word").distinct().count()
  }

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("first example")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.read.option("header", true).csv("D:\\SparkProject\\data-sparkguide\\sentences.csv")
    val tokenized = tokenizeWords(df)

    val overall = overallWordCount(tokenized)
    overall.show()

    val topWords = topFrequentWords(overall, 5)
    topWords.show()

    val sentenceCounts = sentenceWiseWordCount(df)
    sentenceCounts.show()

   


  }
}
