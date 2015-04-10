package com.stratio.model.spark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.language.implicitConversions
import com.stratio.model.Flight
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class FlightCsvReader(self: RDD[String]) {

  private def toParsedFlight: RDD[Either[Seq[(String,String)], String]] ={
    val header =  self.first()
    self.filter(!_.contains(header)).map(linea=> Flight.errorOrHealthy(linea, Flight.extractErrors(linea.split(","))))
  }

//  def wrongFlight: RDD[(String, String)]= toParsedFlight.filter(_.isLeft).flatMap(_.left.get)
//
//  def correctFlight: RDD[Flight]= toParsedFlight.filter(_.isRight).map(_.right.get).map(lineCorrect => Flight.apply(lineCorrect.split(",")))
//
//  /*************************************************************************/

    /**
     *
     * Parser the csv file with the format described in te readme.md file to a Fligth class
     *
     */
    def toFlight: RDD[Flight] = toParsedFlight.filter(_.isRight).map(_.right.get).map(lineCorrect => Flight.apply(lineCorrect.split(",")))

    /**
     *
     * Obtain the parser errors
     *
     */
    def toErrors: RDD[(String, String)] = toParsedFlight.filter(_.isLeft).flatMap(_.left.get)
  }

  class FlightFunctions(self: RDD[Flight]) {

    var sc = new SparkContext("local", "exampleApp")

    def meanDistanceWithAggregate: Double= {
      val result1 = self.map(flight=> flight.distance).aggregate((0,0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

      result1._1.toDouble/result1._2
    }

    //def meanDistance: Double = self.map(flight=> flight.distance).mean()

    def meanDistanceWithMapReduce: Double = {
      val result3 = self.map(flight => (flight.distance, 1)).reduce((dato1, dato2) => (dato1._1 + dato2._1, dato1._2 + dato2._2))
      result3._1.toDouble / result3._2
    }

    def meanDistanceWithGroupByKey: Double = {
      val result4= self.map(flight => (1, flight.distance)).groupByKey.mapValues(distance => distance.toList.reduce(_ + _).toDouble / distance.toList.size)
      result4.reduce((a,b)=> b)._2
    }

    def monthMinPriceByAirportWithBroadcast(implicit broadcastFuelPrice: Broadcast[Map[(Int, Int), Int]]): RDD[(String, (Int, Int))]= {

      val distanceByYearMothAirport = self.map(flight => ((flight.date.getYear, flight.date.getMonthOfYear, flight.origin), flight.distance)).reduceByKey(_ + _)
      val airportDistanceByYearMonth = distanceByYearMothAirport.map(dxairport => ((dxairport._1._1, dxairport._1._2), (dxairport._1._3, dxairport._2)))

      airportDistanceByYearMonth.map(a => (a._2._1, (a._1._2, a._2._2.toInt * broadcastFuelPrice.value(a._1)))).reduceByKey((a, b) => if (a._2 < b._2) a else b)
    }
    /**************************************************************************************/

    /**
     *
     * Obtain the minimum fuel's consumption using a external RDD with the fuel price by Month
     *
     */
    def minFuelConsumptionByMonthAndAirport(fuelPrice: RDD[String]): RDD[(String, Short)] = {

      val broadcastFuelPrice= sc.broadcast(fuelPrice.map(line=> line.split(",")).map(word=> ((word(0).toInt, word(1).toInt),word(2).toInt)).collect().toMap)

      val distanceByYearMothAirport = self.map(flight => ((flight.date.getYear, flight.date.getMonthOfYear, flight.origin), flight.distance)).reduceByKey(_ + _)
      val airportDistanceByYearMonth = distanceByYearMothAirport.map(dxairport => ((dxairport._1._1, dxairport._1._2), (dxairport._1._3, dxairport._2)))

      val monthFuelConsumptionByAirport = airportDistanceByYearMonth.map(a => (a._2._1, (a._1._2.toShort, a._2._2 * broadcastFuelPrice.value(a._1)))).reduceByKey((a, b) => if (a._2 < b._2) a else b)
      monthFuelConsumptionByAirport.map(a=> (a._1, a._2._1))

    }

    /**
     *
     * Obtain the average distance fliyed by airport, taking the origin field as the airport to group
     *
     */
    def averageDistanceByAirport: RDD[(String, Float)] = ???

    /**
     *
     * Reasign the dest Airport and destHour to the ghost flights being a ghost flight those whom doesn't
     *
     */
    def asignGhostFlights(elapsedSeconds: Int): RDD[Flight] = ???
  }


trait FlightDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
}

object FlightDsl extends FlightDsl

