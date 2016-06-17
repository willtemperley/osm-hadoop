package org.roadlessforest.osm.writable

import java.io.{DataOutput, DataInput}

import org.apache.hadoop.io.Writable

/**
  * A way node, i.e. the coordinates and its position (ordinal) in a way
  *
  * Created by willtemperley@gmail.com on 11-Mar-16.
  */
class ReferencedWayNodeWritable() extends  Writable {

  private[this] var value: (Int, Double, Double) = _

  def set(v: (Int, Double, Double)): Unit = {
    value = v
  }

  def ordinal = value._1
  def x = value._2
  def y = value._3

  override def write(out: DataOutput): Unit = {
    out.writeInt(value._1)
    out.writeDouble(value._2)
    out.writeDouble(value._3)
  }

  override def readFields(in: DataInput): Unit = {
    value = (in.readInt(), in.readDouble(), in.readDouble())
  }
}
