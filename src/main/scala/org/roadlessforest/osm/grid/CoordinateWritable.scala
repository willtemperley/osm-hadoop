package org.roadlessforest.osm.grid

import com.google.common.primitives.{Ints, Longs}
import org.apache.hadoop.io.LongWritable

/**
  * Disguising the coordinate as a long writable allows it easily used as an MR key
  *
  * Created by willtemperley@gmail.com on 29-Mar-16.
  */
class CoordinateWritable extends LongWritable {

  def set(a: Int, b: Int): Unit = {
    val bytes = Ints.toByteArray(a) ++ Ints.toByteArray(b)
    val l = Longs.fromByteArray(bytes)
    super.set(l)
  }

  def indices: (Int, Int) = {
    val x = Longs.toByteArray(super.get())
    val a = Ints.fromByteArray(x.slice(0,4))
    val b = Ints.fromByteArray(x.slice(4,8))
    (a, b)
  }
}
