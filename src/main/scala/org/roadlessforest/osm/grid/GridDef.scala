package org.roadlessforest.osm.grid
import com.vividsolutions.jts.geom.Coordinate

/**
  * Created by willtemperley@gmail.com on 12-Jul-17.
  */
trait GridDef {

  /**
    * Snaps a geographical coordinate to a grid coordinate
    *
    * @param coordinate the coordinate
    * @return
    */
  def snap(coordinate: Coordinate): Coord
}
