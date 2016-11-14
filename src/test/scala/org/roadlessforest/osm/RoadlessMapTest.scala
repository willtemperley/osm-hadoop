package org.roadlessforest.osm

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorExportToWkt, ShapeFileReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Mutation, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{KeyValueSerialization, MutationSerialization, ResultSerialization}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.junit.Test
import org.roadlessforest.osm.buffer.WayBuffer
import org.roadlessforest.osm.writable.WayWritable

/**
  * Created by willtemperley@gmail.com on 14-Nov-16.
  */
class RoadlessMap {

  val data = Bytes.toBytes("d")
  val geom = Bytes.toBytes("geom")

  val key = new LongWritable()
  val mapReduceDriver = getMapReduceDriver


  @Test
  def go(): Unit = {

    setupSerialization(mapReduceDriver)

    val fileInputStream = new FileInputStream(new File("src/test/resources/shp/canary.shp"))
    val shapeFileReader = new ShapeFileReader(fileInputStream)

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
    //
    //        while (shapeFileReader.hasNext()) {
    //        }
    //        for (Result result : wayScanner) {
    //            row.set(result.getRow());
    //        }
    mapReduceDriver.run

//    mapReduceDriver.

  }

  def getMapReduceDriver: MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, Text, ImmutableBytesWritable, Mutation] = {

    //eek!
    val mapReduceDriver =
    MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[LongWritable, WayWritable, ImmutableBytesWritable, Text, ImmutableBytesWritable, Mutation]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set("valueKey", "highway")
    mapReduceDriver.setMapper(new WayBuffer.WayRasterMapper)
    mapReduceDriver.setReducer(new WayBuffer.BufferReducer)
    mapReduceDriver
  }


  protected def setupSerialization(mapReduceDriver: MapReduceDriver[_, _, _, _, _, _]): Configuration = {
    val configuration = mapReduceDriver.getConfiguration
    configuration.setStrings("io.serializations", configuration.get("io.serializations"), classOf[MutationSerialization].getName, classOf[ResultSerialization].getName, classOf[KeyValueSerialization].getName)
    configuration
  }

}
