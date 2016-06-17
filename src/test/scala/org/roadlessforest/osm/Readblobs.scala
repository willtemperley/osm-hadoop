package org.roadlessforest.osm

import java.io.File
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer
import org.openstreetmap.osmosis.pbf.PbfBlobDecoder2

/**
  * Created by willtemperley@gmail.com on 31-May-16.
  *
  */
//TODO make into test
object Readblobs {

  def main(args: Array[String]): Unit = {

    val reader = new SequenceFile.Reader(new Configuration(),
      SequenceFile.Reader.file(new Path(args(0))))

    val dir = new File(args(1))

    val key = new LongWritable()
    val v = new ArrayPrimitiveWritable()

    while (reader.next(key, v)) {

      val blobDecoder = new PbfBlobDecoder2(key.toString, v.get().asInstanceOf[Array[Byte]])

      val l: util.ArrayList[EntityContainer] = new util.ArrayList[EntityContainer]()
      blobDecoder.runAndTrapExceptions(l)

      println("n entities: " + l.size())
    }

    reader.close()
  }

}
