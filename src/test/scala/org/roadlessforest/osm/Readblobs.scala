package org.roadlessforest.osm

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._

/**
  * Created by willtemperley@gmail.com on 31-May-16.
  *
  */
//TODO make into test
object Readblobs extends DecodesOsm {

  def main(args: Array[String]): Unit = {

    val reader = new SequenceFile.Reader(new Configuration(),
      SequenceFile.Reader.file(new Path(args(0))))

    val dir = new File(args(1))

    val key = new LongWritable()
    val v = new ArrayPrimitiveWritable()

    while (reader.next(key, v)) {

      val x = readBlob(v.get().asInstanceOf[Array[Byte]], key.toString)

      println("n entities: " + x.size)
    }

    reader.close()
  }

}
