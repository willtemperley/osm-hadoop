package org.roadlessforest.osm.writable

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

/**
  * A mapreduce compatible representation of a node
  *
  * Created by willtemperley@gmail.com on 11-Mar-16.
  */
class NodeWritable() extends  Writable {

  private[this] var value: (Double, Double) = _

  def set(v: (Double, Double)): Unit = {
    value = v
  }

  def x = value._1
  def y = value._2

  override def write(out: DataOutput): Unit = {
    out.writeDouble(value._1)
    out.writeDouble(value._2)
  }

  override def readFields(in: DataInput): Unit = {
    value = (in.readDouble(), in.readDouble())
  }
}
