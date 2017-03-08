package org.roadlessforest.osm.isea

import java.io.File

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Polygon}
import org.roadlessforest.osm.shp.GeomType

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * Created by willtemperley@gmail.com on 19-Dec-16.
  */
object ParseGen {

  class Acceptor() {

    val geomFact = new GeometryFactory

    var polyEnd = false

    var fileEnd = false

    val coords = new ArrayBuffer[Coordinate]

    var prev: String = ""

    var id: Int = _

    def add(v: String): Unit = {

      if (v.equals("END")) {
        polyEnd = true
        if (prev.equals("END")) {
          fileEnd = true
        }
      }

      prev = v

      if (!polyEnd) {

        val list = v.split(" ").toList

        val head = list.head
        val x = list(1).toDouble
        val y = list(2).toDouble

        if (head.length > 0) {
          id = head.toInt
        } else {
          coords += new Coordinate(x, y)
        }
      }

    }

    def isCrossing: Boolean = {

      for (coordPair <- coords.sliding(2)) {
        val a = coordPair(0)
        val b = coordPair(1)
        if (Math.abs(a.x - b.x) > 180) {
          println(id + " is crossing")
          return true
          //          return null
        }
      }
      false
    }

    def getPolygon: Polygon = {

      val poly = geomFact.createPolygon(coords.toArray)
      polyEnd = false
      coords.clear()
      poly
    }

  }

  def main(args: Array[String]): Unit = {

    val gridName = "isea3h9"

    val acceptor = new Acceptor
    val f = new File("e:/isea3h/" + gridName + ".gen")

    val s = Source.fromFile(f)
    val lines = s.getLines()

//    val sw = new ShapeWriter(geomType = GeomType.Polygon)
//
//    for (elem <- lines) {
//      acceptor.add(elem)
//      if (!acceptor.fileEnd) {
//        if (acceptor.polyEnd) {
//          val isCrossing = acceptor.isCrossing
//
//          val p = acceptor.getPolygon
//          //          if (p != null) {
////          println(p)
//          if (!isCrossing) sw.addFeature(p, Seq(acceptor.id.toString))
//          //          }
//        }
//      }
//    }
//
//    sw.write("e:/isea3h/shp/" + gridName + ".shp")

  }

}
