package org.roadlessforest.osm

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import com.esri.core.geometry.examples.ShapefileGeometryCursor
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
import org.roadlessforest.osm.buffer.{RoadlessMap, RoadlessRasterizeMapSide}
import org.roadlessforest.osm.writable.WayWritable

/**
  * Created by willtemperley@gmail.com on 14-Nov-16.
  */
class RoadlessMapTest {

  val data = Bytes.toBytes("d")
  val geom = Bytes.toBytes("geom")

  val key = new LongWritable()
//  val mapReduceDriver = mapSideRasterizer


  @Test
  def reduceSideTest(): Unit = {
    go(reduceSideRasterizer)
  }

  @Test
  def mapSideTest(): Unit = {
    go(mapSideRasterizer)
  }

  def go(mapReduceDriver: MapReduceDriver[LongWritable,WayWritable, _, _, _, _]): Unit = {

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
    mapReduceDriver.setMapper(new RoadlessMap.WayRasterMapper)
    mapReduceDriver.setReducer(new RoadlessMap.BufferReducer)
    mapReduceDriver
  }

  def mapSideRasterizer: MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation] = {

    //eek!
    val mapReduceDriver =
    MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set("valueKey", "highway")
    mapReduceDriver.setMapper(new RoadlessRasterizeMapSide.WayRasterMapper)
    mapReduceDriver.setReducer(new RoadlessRasterizeMapSide.BufferReducer)
    mapReduceDriver
  }


  protected def setupSerialization(mapReduceDriver: MapReduceDriver[_, _, _, _, _, _]): Configuration = {
    val configuration = mapReduceDriver.getConfiguration
    configuration.setStrings("io.serializations", configuration.get("io.serializations"), classOf[MutationSerialization].getName, classOf[ResultSerialization].getName, classOf[KeyValueSerialization].getName)
    configuration
  }

}
