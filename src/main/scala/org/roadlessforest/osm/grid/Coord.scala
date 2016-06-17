package org.roadlessforest.osm.grid

import com.google.common.primitives.{Ints, Longs}

/**
  *
  * Implemented as a value type to minimise overhead whilst keeping readability.
  *
  * Created by willtemperley@gmail.com on 02-Mar-16.
  */
class Coord(val value: (Int, Int)) extends AnyVal with Serializable {
  def x = value._1
  def y = value._2

  def asLong:Long =  {
    val bytes = Ints.toByteArray(x) ++ Ints.toByteArray(y)
    Longs.fromByteArray(bytes)
  }

}