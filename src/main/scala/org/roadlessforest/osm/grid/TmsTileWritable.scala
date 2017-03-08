package org.roadlessforest.osm.grid

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import xyz.tms.TmsTile

/**
  * Disguising as a bytewritable means very little needs implementing
  *
  *
  * Created by willtemperley@gmail.com on 29-Mar-16.
  */
class TmsTileWritable extends ImmutableBytesWritable {


  def set(tile: TmsTile): Unit = {
    this.set(tile.encode())
  }

  def getTile: TmsTile = {
    new TmsTile(this.get)
  }
}
