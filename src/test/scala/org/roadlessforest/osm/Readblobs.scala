package org.roadlessforest.osm

import java.io.{File, FileOutputStream}

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

    val key = new Text()
    val v = new ArrayPrimitiveWritable()

    var i = 0
    while (reader.next(key, v)) {

      println(key.getBytes)
      println(v.get())
      val bytes = v.get().asInstanceOf[Array[Byte]]
//      val x = readBlob(bytes, key.toString)

      val f = new File("E:/tmp/vn11/" + i + ".tif")
      val outputStream = new FileOutputStream(f)
      outputStream.write(bytes)
//      println("n entities: " + x.size)
      i += 1
    }

    reader.close()
  }

}
