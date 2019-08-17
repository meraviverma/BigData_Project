package SparkDefGuide.structfield
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.matching.Regex


/*
Q) we have a text file as 1,ravi@gaya id,name@city .
create a dataframe and store the data in the cassandra.
 */
case class Emp(name:String,sal:Int)
case class Employee(id:Int,name:String,city:String)

object Demo1 {

  def main(arg: Array[String]):Unit={

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val sc=SparkSession
      .builder()
      .appName("CassandraExample")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")

    val abc=Seq(1,"ravi")
    val data=sc.sparkContext.parallelize(abc)
    println(data.collect().mkString(","))
    //data.foreach(println)

    val data2: RDD[String] =sc.sparkContext.textFile("D:\\mypro\\spark\\data-sparkguide\\Employee.txt")

    val inputdata =sc.sparkContext.textFile("D:\\mypro\\spark\\data-sparkguide\\Employee.txt")
      .map(_.replaceAll("@",",")).map(_.split(",")).map(p=>Employee(p(0).toInt,p(1),p(2))).toDF()
    //inputdata.foreach(println)
    // println(inputdata.collect().mkString(","))
    inputdata.show()
    //val data2DF =sc.sparkContext.textFile("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\Employee.txt").toDF("id","name","city")
    /*val data1=Array(10,20,30,40)
    val rdd1=sc.sparkContext.parallelize(data1)
    rdd1.foreach(println)
    println(rdd1.collect().mkString(","))
    val rdd2: RDD[Int] =rdd1.map(_*2)
    println(rdd2.collect().mkString(","))*/

    /*OUTPUT------
    10
    20
    30
    40
    10,20,30,40
    20,40,60,80
     */


    //data2.foreach(println)

    //Ways to create Dataframe

    val somedata=Seq((1,"ravi"),(2,"sam"),(3,"ram"))
    val datadf1=sc.sparkContext.parallelize(somedata).toDF("id","name")

    datadf1.printSchema()
    datadf1.show()

    println("#################### DataFrame 1 Using Case Class ####################")

    val somedata2: DataFrame =sc.sparkContext.textFile("D:\\mypro\\spark\\data-sparkguide\\Emp1.txt")
      .map(_.split(",")).map(p=>Emp(p(0),p(1).toInt)).toDF()
    somedata2.show()

    println("#################### DataFrame 2 ####################")
    val somedata3 =sc.sparkContext.textFile("D:\\mypro\\spark\\data-sparkguide\\emp.txt")

    val schemaString="name sal"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    val rowRDD = somedata3.map(_.split(",")).map(e ⇒ Row(e(0), e(1)))
    val employeeDF = sc.createDataFrame(rowRDD, schema)
    employeeDF.show()

    println("#################### DataFrame 3 ####################")
    val ownschema=StructType(
      StructField("name",StringType,true)::
        StructField("id",IntegerType,true)::Nil
    )
    val datawithschema: DataFrame =sc.read.option("header",false)
      .schema(ownschema)
      .csv("D:\\mypro\\spark\\data-sparkguide\\emp.txt")
    datawithschema.show()

    println("#################### DataFrame 4 ####################")
    val schema1=new StructType().add("name",StringType,true).add("id",IntegerType,true)
    val data1=somedata3.map(_.split(",")).map(e=> Row(e(0),e(1)))
    val empdf=sc.createDataFrame(data1,schema)
    empdf.show()

    //converting dataframe to JSON
    val rdd_json=empdf.toJSON
    rdd_json.take(2).foreach(println)

    println("\n#################### DataFrame 5 Using StructField and StructType ####################")

    val tblstr=new StructType(
      Array(StructField("name",StringType,nullable = true),StructField("id",IntegerType,nullable = true))
    )
    val updateddata=somedata3.map(_.split(",")).map(e=>Row(e(0),e(1).toInt))
    val updateddf=sc.createDataFrame(updateddata,tblstr)

    updateddf.show()

    //compare schema of two dataframe
    println(employeeDF.schema == inputdata.schema )

    //compare the data from two dataframe
    somedata2.except(datawithschema).show()
  }
}
//output
/*
1,ravi
+---+----+-----+
| id|name| city|
+---+----+-----+
|  1|ravi| gaya|
|  2| sam|patna|
+---+----+-----+

root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)

+---+----+
| id|name|
+---+----+
|  1|ravi|
|  2| sam|
|  3| ram|
+---+----+

#################### DataFrame 1 Using Case Class ####################
+------+----+
|  name| sal|
+------+----+
|   Sai|3000|
|  Hari|5000|
|Mahesh|6000|
|   ram| 393|
|  ravi|3636|
+------+----+

#################### DataFrame 2 ####################
+------+----+
|  name| sal|
+------+----+
|   Sai|3000|
|  Hari|5000|
|Mahesh|6000|
+------+----+

#################### DataFrame 3 ####################
+------+----+
|  name|  id|
+------+----+
|   Sai|3000|
|  Hari|5000|
|Mahesh|6000|
+------+----+

#################### DataFrame 4 ####################
+------+----+
|  name| sal|
+------+----+
|   Sai|3000|
|  Hari|5000|
|Mahesh|6000|
+------+----+

{"name":"Sai","sal":"3000"}
{"name":"Hari","sal":"5000"}

#################### DataFrame 5 Using StructField and StructType ####################
+------+----+
|  name|  id|
+------+----+
|   Sai|3000|
|  Hari|5000|
|Mahesh|6000|
+------+----+

false
+----+----+
|name| sal|
+----+----+
|ravi|3636|
| ram| 393|
+----+----+


Process finished with exit code 0

 */