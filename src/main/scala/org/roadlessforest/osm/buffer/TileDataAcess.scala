package org.roadlessforest.osm.buffer

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by willtemperley@gmail.com on 16-Nov-16.
  */
object TileDataAcess {

  val roadCount = Bytes.toBytes("c")
  val imageCol = Bytes.toBytes("i")
  val cf = Bytes.toBytes("d")

}
