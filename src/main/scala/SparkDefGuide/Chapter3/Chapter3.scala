package SparkDefGuide.Chapter3

import conf.SparkConfiguration

object Chapter3 extends SparkConfiguration{
  case class Flight(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:BigInt)
  def main(args: Array[String]):Unit={

    import sc.implicits._
    val flightDF=sc.read.parquet("D:\\mypro\\spark\\data-sparkguide\\2010-summary.parquet")
    val flights=flightDF.as[Flight]

    flights
      .filter(flight_row=> flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row=>flight_row)
      .show(5)

    flights
      .take(5)
      .filter(flight_row=>flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr=> Flight(fr.DEST_COUNTRY_NAME,fr.ORIGIN_COUNTRY_NAME,fr.count + 5))

    //Structure Streaming



  }

}
