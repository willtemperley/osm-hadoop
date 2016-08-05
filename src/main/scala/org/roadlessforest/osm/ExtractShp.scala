package org.roadlessforest.osm

import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, SequenceFile, Text, Writable}
import org.roadlessforest.osm.config.ConfigurationFactory
import org.roadlessforest.osm.shp.{GeomType, ShapeWriter}
import org.roadlessforest.osm.writable.WayWritable

/**
  * Created by willtemperley@gmail.com on 31-May-16.
  *
  *
  *
  */
object ExtractShp {

  val wkt = new WKTReader()

  def main(args: Array[String]): Unit = {

    val configuration = ConfigurationFactory.get
    val path = new Path(args(0))
    val outPath = args(1)

    val fs = path.getFileSystem(configuration)
    fs.listStatus(path).foreach(f =>
      convertFile(configuration, f.getPath, outPath + f.getPath.getName)
    )

  }

  def convertFile(configuration: Configuration, path: Path, outPath: String): Unit = {

    val sw = new ShapeWriter(geomType = GeomType.LineString)

    println(path.toString)
    println(outPath)

    val reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path))

    val key = new LongWritable()
    val v = new WayWritable()

    while (reader.next(key, v)) {
      val ls = wkt.read(v.get(WayWritable.geometry).toString)
      val hwy = v.get(new Text("highway")).toString
      if (hwy != null && hwy.isEmpty) println(hwy)
      sw.addFeature(ls, Seq("test"))
    }

    sw.write(outPath + ".shp")
    reader.close()
  }

}
