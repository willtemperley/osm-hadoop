package org.roadlessforest.osm.grid

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import xyz.mercator.MercatorTile

/**
  * Created by willtemperley@gmail.com on 02-Mar-17.
  */
class MercatorTileWritable extends ImmutableBytesWritable {

  def setTile(tile: MercatorTile): Unit = {
    set(tile.getX, tile.getY, tile.getZ)
  }

  def set(x: Int, y: Int, z: Int): Unit = {

    val buffer = ByteBuffer.wrap(new Array[Byte](12))

    buffer.putInt(x)
    buffer.putInt(y)
    buffer.putInt(z)

    val arr = buffer.array()
    this.set(arr)
  }

  def getTile(tile: MercatorTile): MercatorTile = {
    val arr = ByteBuffer.wrap(this.get())
    tile.setX(arr.getInt())
    tile.setY(arr.getInt())
    tile.setZ(arr.getInt())
    tile
  }

//  def getXY: (Int, Int) = {
//    val arr = ByteBuffer.wrap(this.get())
//    (arr.getInt(), arr.getInt())
//  }
}
