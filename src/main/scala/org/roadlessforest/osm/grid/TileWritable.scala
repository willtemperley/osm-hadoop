package org.roadlessforest.osm.grid

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import xyz.TileCalculator.Tile

/**
  * Disguising as a bytewritable means very little needs implementing
  *
  * Created by willtemperley@gmail.com on 29-Mar-16.
  */
class TileWritable extends ImmutableBytesWritable {

  def set(tile: Tile): Unit = {
    this.set(tile.encode())
  }

  def getTile: Tile = {
    new Tile(this.get)
  }

}
