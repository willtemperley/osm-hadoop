package org.roadlessforest.osm.writable

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

/**
  *
  *
  * Created by willtemperley@gmail.com on 11-Mar-16.
  */
class WayNodeWritable() extends Writable {

  private[this] var value: (Long, Int) = _

  def set(v: (Long, Int)): Unit = value = v

  def wayId = value._1

  def ordinal = value._2

  override def write(out: DataOutput): Unit = {
    out.writeLong(value._1)
    out.writeInt(value._2)
  }

  override def readFields(in: DataInput): Unit = {
    value = (in.readLong(), in.readInt())
  }
}
