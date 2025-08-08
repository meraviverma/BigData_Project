package src.main.scala.InterviewQuestions.capgemini

import conf.SparkConfiguration

object SparkCapgemini extends SparkConfiguration {
  case class Person(name:String,age:Long)
  case class fruitprice(fruit:String,price:Long)
  def main(args: Array[String]):Unit={

    import  sc.implicits._

    println("Hello")

    var fruits=List("apple","mangoes")
    var price=List(10,20)

    //Create a dataframe out of it
    //var fruitprocedf=Seq(fruits)


    val data=Seq(Person("ravi",29),Person("sam",30))

    var mydf=sc.createDataFrame(data)
    mydf.printSchema()


    val datacourse =Seq(("java", "dbms", "python"), ("OOPS", "SQL", "Machine Learning"))
    val columns = Seq("Subject 1", "Subject 2", "Subject 3")
//
    val mycoursedf=sc.createDataFrame(datacourse).toDF(columns:_*)
    mycoursedf.printSchema()



  }

}
