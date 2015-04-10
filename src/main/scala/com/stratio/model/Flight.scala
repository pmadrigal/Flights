package com.stratio.model

import com.stratio.utils.ParserUtils
import org.apache.spark.AccumulableParam
import org.joda.time.DateTime


sealed class Cancelled

object OnTime extends Cancelled
object Cancel extends Cancelled
object Unknown extends Cancelled

case class Delays (
                    carrier: Cancelled,
                    weather: Cancelled,
                    nAS: Cancelled,
                    security: Cancelled,
                    lateAircraft: Cancelled)

case class Flight (date: DateTime, //Tip: Use ParserUtils.getDateTime

    departureTime: Int,
    crsDepatureTime: Int,
    arrTime: Int,
    cRSArrTime: Int,
    uniqueCarrier: String,
    flightNum: Int,
    actualElapsedTime: Int,
    cRSElapsedTime: Int,
    arrDelay: Int,
    depDelay: Int,
    origin: String,
    dest: String,
    distance: Int,
    cancelled: Cancelled,
    cancellationCode: Int,
    delay: Delays)

object Flight{

  /*
  *
  * Create a new Flight Class from a CSV file
  *
  */

//  val errors = sc.accumulable(scala.collection.mutable.Map.empty[String, Int])(AccParam)
//  object AccParam extends AccumulableParam[scala.collection.mutable.Map[String, Int], String] {
//
//    def zero(initialValue: scala.collection.mutable.Map[String, Int]) = scala.collection.mutable.Map.empty[String, Int]
//
//    def addInPlace(r1: scala.collection.mutable.Map[String, Int], r2: scala.collection.mutable.Map[String, Int]): scala.collection.mutable.Map[String, Int] = {
//      r2.foreach{ kv => add(r1, kv._1, kv._2) }
//      r1
//    }
//
//    def addAccumulator(result: scala.collection.mutable.Map[String, Int], errorType: String): scala.collection.mutable.Map[String, Int] = {
//      add(result, errorType, 1)
//      result
//    }
//
//    private def add(result: scala.collection.mutable.Map[String, Int], key: String, value: Int): Unit = {
//      result.put(key, result.getOrElse(key, 0) + value)
//    }
//  }


  def apply(fields: Array[String]): Flight = {

    val (firstChuck, secondChuck) = fields.splitAt(22)
    val Array(year, month, dayOfMonth,dayOfWeek, departureTime, crsDepatureTime, arrTime, cRSArrTime,
    uniqueCarrier, flightNum, _, actualElapsedTime, cRSElapsedTime,airTime, arrDelay,
    depDelay, origin, dest, distance, _, _, cancelled) = firstChuck
    val Array(cancellationCode, diverted, carrier, weather, nAS, security, lateAircraft) = secondChuck

    val delays = Delays(parseCancelled(carrier),parseCancelled(weather),parseCancelled(nAS),parseCancelled(security),
      parseCancelled(lateAircraft))

    Flight(
      ParserUtils.getDateTime(year.toInt,month.toInt,dayOfMonth.toInt),
      departureTime.toInt,
      crsDepatureTime.toInt,
      arrTime.toInt,
      cRSArrTime.toInt,
      uniqueCarrier,
      flightNum.toInt,
      actualElapsedTime.toInt,
      cRSElapsedTime.toInt,
      arrDelay.toInt,
      depDelay.toInt,
      origin,
      dest,
      distance.toInt,
      parseCancelled(cancelled),
      cancellationCode.toInt,
      delays
    )
  }
  /*
   *
   * Extract the different types of errors in a string list
   *
   */
  def extractErrors(fields: Array[String]): Seq[String] =
  {
    val intColumn = Seq(0,1,2,3,4,5,6,7,9,11,12,14,15,18,21,23)
    val stringColumn = Seq(8,16,17)
    val NaColumn = Seq(10,13,19,20,22,24,25,26,27,28)

    val integers=  intColumn.flatMap(column => tryInt(fields(column)))
    val strings=  stringColumn.flatMap(column => tryString(fields(column)))
    val na=  NaColumn.flatMap(column =>tryNa(fields(column)))

    val output=(integers ++ strings ++ na)

    output //puede que con esto solo valga

//    if(!(output ).isEmpty)
//      (output)
//    else
//      Seq()

  }

  def errorOrHealthy(linea : String, input : Seq[String]): Either[Seq[(String,String)], String]= {

    if(!(input ).isEmpty)
      Left(input.map(in=> Seq(linea).zip(Seq(in))).flatMap(out=> out))
    else
      Right(linea)
  }


  private def tryNa (field: String): Option[String]= {

    if (field.compareTo("NA") == 0)
      None
    else {
      //errors.add("ErrorTipo3")
      Some(("ErrorTipo3"))
    }
  }

  private def tryString (field: String): Option[String]={
    if(field.compareTo("NA")==0)
    {
      //errors.add("ErrorTipo1")
      Some(("ErrorTipo1"))
    }
    else
      None
  }

  private def tryInt(field: String): Option[String]={
    try{
      if(field.compareTo("NA")==0)
      { //errors.add("ErrorTipo1")
        Some(("ErrorTipo1"))
      }
      else
      {
        field.toInt
        None
      }
    } catch {
      case e: Exception =>
        //errors.add("ErrorTipo2")
        Some(("ErrorTipo2"))
    }
  }
  /*
  *
  * Parse String to Cancelled Enum:
  *   if field == 1 -> Cancel
  *   if field == 0 -> OnTime
  *   if field <> 0 && field<>1 -> Unknown
  */
  def parseCancelled(field: String): Cancelled = field.toString match {
    case "1" => Cancel
    case "0" => OnTime
    case  _   => Unknown
  }

}
