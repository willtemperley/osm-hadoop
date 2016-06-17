package org.roadlessforest.osm

import java.io.{ByteArrayInputStream, DataInputStream}

import com.sun.corba.se.spi.ior.Writeable
import org.apache.hadoop.io.{Writable, WritableUtils}
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.TableDrivenPropertyChecks

/**
  * Created by willtemperley@gmail.com on 30-Mar-16.
  */
trait WritableSpec extends PropSpec with TableDrivenPropertyChecks with Matchers {

  /*
  Just writes the first argument to binary and reads it into the second
   */
  def serializeDeserialize(writable: Writable, emptyWritable: Writable): Unit = {

    val serialized = WritableUtils.toByteArray(writable)
    val dis = new DataInputStream(new ByteArrayInputStream(serialized))
    emptyWritable.readFields(dis)

  }


}
