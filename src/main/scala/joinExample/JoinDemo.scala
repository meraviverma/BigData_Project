package joinExample

import SparkDefGuide.Chapter5.Chapter5.sc
import conf.SparkConfiguration
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

import java.lang.Thread.sleep

object JoinDemo extends SparkConfiguration {
  def main(args: Array[String]):Unit= {

    import sc.implicits._

    val myschema=StructType(
      StructField("id",StringType,true)::Nil
    )
    //val t1=sc.sparkContext.parallelize(Seq(1,2,3,4))
    //val mydf=sc.createDataFrame(t1,myschema)

    val somedata1 =Seq((1,"ravi"),(2,"sam"),(3,null),(1,null))
    val somedata2=Seq((1,"ravi"),(1,"sam"),(3,"sam"),(1,null))
    val datadf1=sc.sparkContext.parallelize(somedata1).toDF("id","name")
    datadf1.createOrReplaceTempView("tableA")

    val datadf2=sc.sparkContext.parallelize(somedata2).toDF("id","name")
    datadf2.createOrReplaceTempView("tableB")

    sc.sql("select t1.name,t2.name from tableA t1 inner join tableB t2 where t1.name=t2.name").show()
    sc.sql("select t1.name,t2.name from tableA t1 left join tableB t2 where t1.name=t2.name")

    println(sc.conf.get("spark.sql.shuffle.partitions")) //200 when set to local

    println(sc.sparkContext.defaultParallelism) //1 //1 when set to local

    //datadf1.printSchema()
    //datadf1.count()
    //print(datadf1.count())
    //datadf2.show()

    sleep(50000000)


  }
}
