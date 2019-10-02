package SparkDefGuide.Chapter8

import conf.SparkConfiguration
import org.apache.spark.sql.functions._
/*
Join Expression
equi-join: compares whether the specified keys in your left and right
dataset are equal,if they are equal,spark will combine left and right dataset

Join types:
a) inner joins (keep rows with keys that exist in the left and right datasets),
b) outer joins (keep rows with keys in either the left or right datasets),
c) left outer joins (keep rows with keys in the left dataset),
d) right outer joins (keep rows with keys in the right dataset),
e) left semi joins (keep the rows in the left (and only the left) dataset where
the key appears in the right dataset),
f) left anti joins (keep the rows in the left (and only the left) dataset where
they does not appear in the right dataset)
g) cross (or cartesian) joins (match every row in the left dataset with every
row in the right dataset).

 */
object Chapter8 extends  SparkConfiguration{
  def main(args: Array[String]): Unit = {

    import sc.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")

    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    println(" ############ Person ################")
    person.show()
    println(" ############ Graduate Program ################")
    graduateProgram.show()
    println(" ############ sparkStatus ################")
    sparkStatus.show()

    println(" ############ Inner Joins ################")
    //Inner joins evaluate the keys in both of the dataframe or tables and include only the rows that evaluate to true.

    val joinExpression=person.col("graduate_program") === graduateProgram.col("id")

    person.join(graduateProgram,joinExpression).show()

    var joinType="outer"
    println("############ Outer Joins ########################")
    person.join(graduateProgram,joinExpression,joinType).show()

    println("############ Left Outer Joins ########################")
    joinType="left_outer"
    graduateProgram.join(person,joinExpression,joinType).show()

    println("############ Right Outer Joins ########################")
    joinType="right_outer"
    person.join(graduateProgram,joinExpression,joinType).show()

    println("############ Left semi join ########################")
    joinType="left_semi"
    graduateProgram.join(person,joinExpression,joinType).show()

    println("############ Left Anti join ########################")
    joinType="left_anti"
    graduateProgram.join(person,joinExpression,joinType).show()

    println("############ Cross join ########################")
    joinType="cross"
    graduateProgram.join(person,joinExpression,joinType).show()

    println("########### Joins on Complex Types #############")
    person.withColumnRenamed("id","personId")
      .join(sparkStatus,expr("array_contains(spark_status,id)")).show()

    println("################ Handling Duplicate column names ###############")

    val gradProgramDupe=graduateProgram.withColumnRenamed("id","graduate_program")
    val joinExpr=gradProgramDupe.col("graduate_program") === person.col("graduate_program")

    person.join(gradProgramDupe,joinExpr).show()

    //Approach1: When you have two keys that have same name. probably the easiest fix is to change the join
    //expression from a Boolean expression to a String or sequence.This automatically removes one of the columns for u
    person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
    person.join(gradProgramDupe,"graduate_program").show()

    //Approach2: Dropping  the column after the join
    //Drop the offending column after the join
    person.join(gradProgramDupe,joinExpr).drop(person.col("graduate_program")).select("graduate_program").show()
    person.join(gradProgramDupe,joinExpr).drop(person.col("graduate_program")).show()

    println("###################### BroadCast Join ####################")
    val joinExpr2=person.col("graduate_program") === graduateProgram.col("id")
    person.join(broadcast(graduateProgram),joinExpr2).explain()


    /* ################# OUTPUT ##################

     ############ Person ################
+---+----------------+----------------+---------------+
| id|            name|graduate_program|   spark_status|
+---+----------------+----------------+---------------+
|  0|   Bill Chambers|               0|          [100]|
|  1|   Matei Zaharia|               1|[500, 250, 100]|
|  2|Michael Armbrust|               1|     [250, 100]|
+---+----------------+----------------+---------------+

 ############ Graduate Program ################
+---+-------+--------------------+-----------+
| id| degree|          department|     school|
+---+-------+--------------------+-----------+
|  0|Masters|School of Informa...|UC Berkeley|
|  2|Masters|                EECS|UC Berkeley|
|  1|  Ph.D.|                EECS|UC Berkeley|
+---+-------+--------------------+-----------+

 ############ sparkStatus ################
+---+--------------+
| id|        status|
+---+--------------+
|500|Vice President|
|250|    PMC Member|
|100|   Contributor|
+---+--------------+

 ############ Inner Joins ################
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+

############ Outer Joins ########################
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
|   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+

############ Left Outer Joins ########################
+---+-------+--------------------+-----------+----+----------------+----------------+---------------+
| id| degree|          department|     school|  id|            name|graduate_program|   spark_status|
+---+-------+--------------------+-----------+----+----------------+----------------+---------------+
|  0|Masters|School of Informa...|UC Berkeley|   0|   Bill Chambers|               0|          [100]|
|  2|Masters|                EECS|UC Berkeley|null|            null|            null|           null|
|  1|  Ph.D.|                EECS|UC Berkeley|   2|Michael Armbrust|               1|     [250, 100]|
|  1|  Ph.D.|                EECS|UC Berkeley|   1|   Matei Zaharia|               1|[500, 250, 100]|
+---+-------+--------------------+-----------+----+----------------+----------------+---------------+

############ Right Outer Joins ########################
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
|   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+

############ Left semi join ########################
+---+-------+--------------------+-----------+
| id| degree|          department|     school|
+---+-------+--------------------+-----------+
|  0|Masters|School of Informa...|UC Berkeley|
|  1|  Ph.D.|                EECS|UC Berkeley|
+---+-------+--------------------+-----------+

############ Left Anti join ########################
+---+-------+----------+-----------+
| id| degree|department|     school|
+---+-------+----------+-----------+
|  2|Masters|      EECS|UC Berkeley|
+---+-------+----------+-----------+

############ Cross join ########################
+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
| id| degree|          department|     school| id|            name|graduate_program|   spark_status|
+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
|  0|Masters|School of Informa...|UC Berkeley|  0|   Bill Chambers|               0|          [100]|
|  1|  Ph.D.|                EECS|UC Berkeley|  2|Michael Armbrust|               1|     [250, 100]|
|  1|  Ph.D.|                EECS|UC Berkeley|  1|   Matei Zaharia|               1|[500, 250, 100]|
+---+-------+--------------------+-----------+---+----------------+----------------+---------------+

########### Joins on Complex Types #############
+--------+----------------+----------------+---------------+---+--------------+
|personId|            name|graduate_program|   spark_status| id|        status|
+--------+----------------+----------------+---------------+---+--------------+
|       0|   Bill Chambers|               0|          [100]|100|   Contributor|
|       1|   Matei Zaharia|               1|[500, 250, 100]|500|Vice President|
|       1|   Matei Zaharia|               1|[500, 250, 100]|250|    PMC Member|
|       1|   Matei Zaharia|               1|[500, 250, 100]|100|   Contributor|
|       2|Michael Armbrust|               1|     [250, 100]|250|    PMC Member|
|       2|Michael Armbrust|               1|     [250, 100]|100|   Contributor|
+--------+----------------+----------------+---------------+---+--------------+

################ Handling Duplicate column names ###############
+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status|graduate_program| degree|          department|     school|
+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|               0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+

+----------------+
|graduate_program|
+----------------+
|               0|
|               1|
|               1|
+----------------+

+----------------+---+----------------+---------------+-------+--------------------+-----------+
|graduate_program| id|            name|   spark_status| degree|          department|     school|
+----------------+---+----------------+---------------+-------+--------------------+-----------+
|               0|  0|   Bill Chambers|          [100]|Masters|School of Informa...|UC Berkeley|
|               1|  2|Michael Armbrust|     [250, 100]|  Ph.D.|                EECS|UC Berkeley|
|               1|  1|   Matei Zaharia|[500, 250, 100]|  Ph.D.|                EECS|UC Berkeley|
+----------------+---+----------------+---------------+-------+--------------------+-----------+

+----------------+
|graduate_program|
+----------------+
|               0|
|               1|
|               1|
+----------------+

+---+----------------+---------------+----------------+-------+--------------------+-----------+
| id|            name|   spark_status|graduate_program| degree|          department|     school|
+---+----------------+---------------+----------------+-------+--------------------+-----------+
|  0|   Bill Chambers|          [100]|               0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|     [250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|[500, 250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+---------------+----------------+-------+--------------------+-----------+

###################### BroadCast Join ####################
== Physical Plan ==
*(1) BroadcastHashJoin [graduate_program#11], [id#26], Inner, BuildRight
:- LocalTableScan [id#9, name#10, graduate_program#11, spark_status#12]
+- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
   +- LocalTableScan [id#26, degree#27, department#28, school#29]
     */






  }
}
