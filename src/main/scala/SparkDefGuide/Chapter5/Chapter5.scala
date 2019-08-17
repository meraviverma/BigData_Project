package SparkDefGuide.Chapter5

import conf.SparkConfiguration
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.types._
/*
read.format("json")
printschema
schema
StructType
StructField
 */
object Chapter5 extends SparkConfiguration {
  def main(args: Array[String]):Unit={

    val df=sc.read.format("json").load("D:\\mypro\\spark\\data-sparkguide\\2015-summary.json")
    df.printSchema()

    sc.read.format("json").load("D:\\mypro\\spark\\data-sparkguide\\2015-summary.json").schema

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME",StringType,true),
      StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      StructField("count",LongType,false,
        Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df1=sc.read.format("json").schema(myManualSchema).load("D:\\mypro\\spark\\data-sparkguide\\2015-summary.json")
    df1.printSchema()

    //comparing tow schema
    val schemaUntyped = new StructType()
      .add("a", "int")
      .add("b", "string")
    val schemaTyped = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)

    println(schemaUntyped == schemaTyped)






  }

}
