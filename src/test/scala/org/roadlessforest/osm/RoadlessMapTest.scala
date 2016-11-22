package org.roadlessforest.osm

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer

import com.esri.core.geometry.examples.ShapefileGeometryCursor
import xyz.TileCalculator.Tile

//import com.esri.core.geometry.examples.ShapefileGeometryCursor
import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorExportToWkt}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Mutation, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{KeyValueSerialization, MutationSerialization, ResultSerialization}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.junit.Test
import org.roadlessforest.osm.buffer.{RoadlessRasterizeMapSide, RoadlessRasterizeReduceSide, RoadlessRoadCount}
import org.roadlessforest.osm.writable.WayWritable

/**
  * Created by willtemperley@gmail.com on 14-Nov-16.
  */
class RoadlessMapTest {

  val data = Bytes.toBytes("d")
  val geom = Bytes.toBytes("geom")

  val key = new LongWritable()
//  val mapReduceDriver = mapSideRasterizer

//    @Test
//    def mapSideTest(): Unit = {
//      executeMR(mapSideRasterizer2)
//    }

  @Test
  def mapSideTest(): Unit = {
    executeMR(mapSideRasterizer)
  }
//
//  @Test
//  def reduceSideTest(): Unit = {
//    executeMR(reduceSideRasterizer)
//  }


  def executeMR(mapReduceDriver: MapReduceDriver[LongWritable,WayWritable, _, _, _, _]): Unit = {

    setupSerialization(mapReduceDriver)

    val fileInputStream = new FileInputStream(new File("src/test/resources/shp/canary.shp"))
    val shapeFileReader = new ShapefileGeometryCursor(fileInputStream)

    //        OperatorImportFromWkb local = OperatorImportFromWkb.local();

    while (shapeFileReader.hasNext) {

      val next = shapeFileReader.next

      key.set(shapeFileReader.getGeometryID)
      val wkt: String = OperatorExportToWkt.local().execute(0, next, null)

      val x = new WayWritable
      x.put("geometry", wkt)
      x.put("highway", "anything")

      mapReduceDriver.withInput(key, x)

    }
    mapReduceDriver.run

  }

  def reduceSideRasterizer: MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, Text, ImmutableBytesWritable, Mutation] = {

    //eek!
    val mapReduceDriver =
    MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, Text, ImmutableBytesWritable, Mutation]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set("valueKey", "highway")
    mapReduceDriver.setMapper(new RoadlessRasterizeReduceSide.TileToWayMapper)
    mapReduceDriver.setReducer(new RoadlessRasterizeReduceSide.BufferReducer)
    mapReduceDriver
  }

  def mapSideRasterizer: MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation] = {

    class TileStack extends RoadlessRasterizeMapSide.RasterizedTileStack {
      override protected def writeDebugTile(key: ImmutableBytesWritable, bytes: Array[Byte]): Unit = {
        val tile = new Tile(key.get)
        println(tile)
        val f: File = new File("e:/tmp/ras/rasterizedtilestack-" + tile.toString + ".png")
        val fileOutputStream: FileOutputStream = new FileOutputStream(f)
        for (aByte <- bytes) {
          fileOutputStream.write(aByte)
        }
      }
    }

    //eek!
    val mapReduceDriver =
    MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set("valueKey", "highway")
    mapReduceDriver.setMapper(new RoadlessRasterizeMapSide.WayRasterMapper)
    mapReduceDriver.setReducer(new TileStack)
    mapReduceDriver
  }


  def mapSideRasterizer2: MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable] = {

    class TileStack2 extends RoadlessRasterizeMapSide.RasterizedTileStack2 {

      override protected def writeDebugTile(key: ImmutableBytesWritable, bytes: Array[Byte]): Unit = {
        val tile = new Tile(key.get)
        println(tile)
        val f: File = new File("e:/tmp/ras/mr-" + tile.toString + ".png")
        val fileOutputStream: FileOutputStream = new FileOutputStream(f)
        for (aByte <- bytes) {
          fileOutputStream.write(aByte)
        }
      }

    }

    val mapReduceDriver =
    MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set("valueKey", "highway")
    mapReduceDriver.setMapper(new RoadlessRasterizeMapSide.WayRasterMapper)
    mapReduceDriver.setReducer(new TileStack2)
    mapReduceDriver
  }


  protected def setupSerialization(mapReduceDriver: MapReduceDriver[_, _, _, _, _, _]): Configuration = {
    val configuration = mapReduceDriver.getConfiguration
    configuration.setStrings("io.serializations", configuration.get("io.serializations"), classOf[MutationSerialization].getName, classOf[ResultSerialization].getName, classOf[KeyValueSerialization].getName)
    configuration
  }

}
