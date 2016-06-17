package org.roadlessforest.osm.grid

import com.vividsolutions.jts.geom.Coordinate


/**
  * Created by willtemperley@gmail.com on 28-May-15.
  *
  */
class GlobalGrid(val w: Int, val h: Int) {

  //TODO think of a way to make this more generic, with less hard-coding
  /**
    * Snaps a geographical coordinate to a grid coordinate
    *
    * @param coordinate the coordinate
    * @return
    */
  def snap(coordinate: Coordinate): Coord = {

    val x = coordinate.getOrdinate(0)
    val y = coordinate.getOrdinate(1)

    val x1 = (((x + 180) * w) / 360).toInt
    val y1 = (((y + 90) * h) / 180).toInt

    new Coord((x1, y1))
  }


}
