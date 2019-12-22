package joinExample

import SparkDefGuide.Chapter5.Chapter5.sc
import conf.SparkConfiguration
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

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
    datadf1.createOrReplaceTempView("t1")

    val datadf2=sc.sparkContext.parallelize(somedata2).toDF("id","name")
    datadf2.createOrReplaceTempView("t2")

    sc.sql("select t1.name,t2.name from t1 inner join t2 where t1.name=t2.name").show()
    sc.sql("select t1.name,t2.name from t1 outer join t2 where t1.name=t2.name").show()



    //datadf1.printSchema()
    datadf1.show()
    datadf2.show()


  }
}
