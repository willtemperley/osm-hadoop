package org.roadlessforest.osm

import xyz.mercator.MercatorTileCalculator

/**
  * Created by willtemperley@gmail.com on 07-Mar-17.
  */
object PixTest {

  def main(args: Array[String]): Unit = {

    val calc = new MercatorTileCalculator

    val tile = calc.tileForCoordinate(0.187, 10.215, 14)

    println(tile)

  }

}
