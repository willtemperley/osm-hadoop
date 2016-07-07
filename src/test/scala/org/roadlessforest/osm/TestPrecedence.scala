package org.roadlessforest.osm

import org.roadlessforest.osm.config.ConfigurationFactory

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 05-Jul-16.
  */
object TestPrecedence {

  val classToPrecedenceMap = Map(
    1 -> 1,//"motorway"
    2 -> 1,//"trunk"
    4 -> 2,//"primary"
    5 -> 3,//"secondary"
    6 -> 4,//"tertiary"
    7 -> 1,//"motorway link"
    8 -> 3,//"primary link"
    9 -> 5,//"unclassified"
    10 -> 5,//"road"
    11 -> 6,//"residential"
    12 -> 7,//"service"
    13 -> 5,//"track"
    14 -> 8,//"pedestrian"
    15 -> 9//"Other"
  ).withDefaultValue(9)


  def main(args: Array[String]): Unit = {

    val props = ConfigurationFactory.getPrecedence

    for (elem <- props) {
      println(elem)
    }

    val x = props.toList.sortBy(f => (f._2, f._1))

    x.foreach(f => print(f._1 + ","))
  }

  def ma(args: Array[String]): Unit = {

    val x = classToPrecedenceMap.toList.sortBy(f => (f._2, f._1))

    x.foreach(f => print(f._1 + ","))

  }

  def m(args: Array[String]) {

    val pixels = Array(7,2,3,4,9,7,3)

    val pixelsToPrecedence: Array[(Int, Int)] = pixels.zip(pixels.map(classToPrecedenceMap))

    val a = pixelsToPrecedence.min

    println(a)

  }

}
