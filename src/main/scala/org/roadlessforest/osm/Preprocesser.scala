package org.roadlessforest.osm

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayPrimitiveWritable, SequenceFile, Text}
import org.openstreetmap.osmosis.osmbinary.Fileformat
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader

/**
  * Binary
  * Created by willtemperley@gmail.com on 31-May-16.
  */
object Preprocesser {

  val k = new Text()
  val v = new ArrayPrimitiveWritable()

  def main(args: Array[String]): Unit = {

    val f = new File(args(0))
    val fileIn = new DataInputStream(new FileInputStream(f))

    val conf = new Configuration
    val fileOption = SequenceFile.Writer.file(new Path(args(1)))

    val out = SequenceFile.createWriter(conf, fileOption,
      SequenceFile.Writer.keyClass(classOf[Text]),
      SequenceFile.Writer.valueClass(classOf[ArrayPrimitiveWritable])
    )

    var i = 0
    while (fileIn.available() > 0) {
      val headerLength = fileIn.readInt
      val blobHeader: BlobHeader = readHeader(headerLength, fileIn)
      val blobData = readRawBlob(blobHeader, fileIn)

      i += 1
      println(i)

      k.set(blobHeader.getType)
      v.set(blobData)

      out.append(k,v)
    }

    out.close()
  }

  private def readHeader(headerLength: Int, fileIn: DataInputStream): Fileformat.BlobHeader = {
    val headerBuffer = new Array[Byte](headerLength)
    fileIn.readFully(headerBuffer)
    val blobHeader = Fileformat.BlobHeader.parseFrom(headerBuffer)
    blobHeader
  }

  @throws(classOf[IOException])
  private def readRawBlob(blobHeader: Fileformat.BlobHeader, fileIn: DataInputStream): Array[Byte] = {
    val rawBlob = new Array[Byte](blobHeader.getDatasize)
    fileIn.readFully(rawBlob)
    rawBlob
  }


}
