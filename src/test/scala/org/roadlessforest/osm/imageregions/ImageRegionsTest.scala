package org.roadlessforest.osm.imageregions

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, SequenceFile}
import org.roadlessforest.osm.MRUnitSerialization
import org.roadlessforest.osm.grid.MercatorTileWritable
import org.roadlessforest.osm.rasterstats.ImageRegions
import org.roadlessforest.xyz.{ImageTileWritable, TileKeyWritable}
import xyz.mercator.MercatorTile

import scala.collection.JavaConversions._

//import com.esri.core.geometry.examples.ShapefileGeometryCursor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.junit.Test

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

  def tileMR: MapReduceDriver[TileKeyWritable, ImageTileWritable, MercatorTileWritable, IntWritable, MercatorTileWritable, Text] = {

    //eek!
    val mapReduceDriver =
      MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[TileKeyWritable, ImageTileWritable, MercatorTileWritable, IntWritable, MercatorTileWritable, Text]]

    setupSerialization(mapReduceDriver)
//    mapReduceDriver.getConfiguration.set(ImageRegions.tmplocationKey, "E:/tmp/imageregions_mr")
    mapReduceDriver.setMapper(new ImageRegions.GeotiffMapper)
    mapReduceDriver.setReducer(new ImageRegions.TileReducer)
    mapReduceDriver
  }

  def executeMR(mapReduceDriver: MapReduceDriver[TileKeyWritable, ImageTileWritable, MercatorTileWritable, IntWritable, MercatorTileWritable, Text]): Unit = {

    setupSerialization(mapReduceDriver)

    val littleTiff = "data/littletiff.seq"

    val resource = getResource(littleTiff)
    val reader = new SequenceFile.Reader(new Configuration(),
      SequenceFile.Reader.file(new Path(resource)))
//    val reader = new SequenceFile.Reader(new Configuration(),
//      SequenceFile.Reader.file(new Path(new File("E:/tmp/vn11/xyz.seq").getPath)))

    val key = new TileKeyWritable()
    val v = new ImageTileWritable()

    while (reader.next(key, v)) {
      mapReduceDriver.withInput(key, v)
    }

    reader.close()

    println("Data read, starting MR")

    val x = mapReduceDriver.run

    val mercTile = new MercatorTile
    for (t <- x) {

      val k = t.getFirst
      val v = t.getSecond

      k.getTile(mercTile)


    }


  }

}
