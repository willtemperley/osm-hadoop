package org.roadlessforest.osm.imageregions

import java.io.{File, FileOutputStream}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayPrimitiveWritable, IntWritable, SequenceFile}
import org.roadlessforest.osm.MRUnitSerialization
import org.roadlessforest.osm.grid.MercatorTileWritable
import org.roadlessforest.osm.rasterstats.ImageRegions

//import com.esri.core.geometry.examples.ShapefileGeometryCursor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Mutation
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.junit.Test
import org.roadlessforest.osm.buffer.RoadlessRasterizeMapSide
import org.roadlessforest.osm.writable.WayWritable

/**
  * Created by willtemperley@gmail.com on 14-Nov-16.
  */
class ImageRegionsTest extends MRUnitSerialization {

  @Test
  def go(): Unit = {
    executeMR(tileMR)
  }

  def exportTinyTiff(): Unit = {

  }

  def tileMR: MapReduceDriver[Text, ArrayPrimitiveWritable, MercatorTileWritable, IntWritable, Text, Text] = {

    //eek!
    val mapReduceDriver =
      MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[Text, ArrayPrimitiveWritable, MercatorTileWritable, IntWritable, Text, Text]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set(ImageRegions.tmplocationKey, "E:/tmp/imageregions_mr")
    mapReduceDriver.setMapper(new ImageRegions.GeotiffMapper)
    mapReduceDriver.setReducer(new ImageRegions.TileReducer)
    mapReduceDriver
  }

  def executeMR(mapReduceDriver: MapReduceDriver[Text, ArrayPrimitiveWritable, _, _, _, _]): Unit = {

    setupSerialization(mapReduceDriver)

    val tinyTiff = "data/tinytiff.seq"

    val resource = getResource(tinyTiff)
    val reader = new SequenceFile.Reader(new Configuration(),
      SequenceFile.Reader.file(new Path(resource)))

    val key = new Text()
    val v = new ArrayPrimitiveWritable()

    while (reader.next(key, v)) {
      mapReduceDriver.withInput(key, v)
    }

    reader.close()

    println("Data read, starting MR")

    mapReduceDriver.run
  }

}
