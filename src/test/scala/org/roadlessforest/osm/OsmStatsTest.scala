package org.roadlessforest.osm

import java.io.{File, FileInputStream}

import com.esri.core.geometry.examples.ShapefileGeometryCursor
import org.apache.hadoop.io.DoubleWritable
import org.openstreetmap.osmosis.hbase.xyz.WebTileWritable

//import com.esri.core.geometry.examples.ShapefileGeometryCursor
import com.esri.core.geometry.OperatorExportToWkt
import org.apache.hadoop.hbase.client.Mutation
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.junit.Test
import org.roadlessforest.osm.writable.WayWritable

/**
  * Created by willtemperley@gmail.com on 14-Nov-16.
  */
class OsmStatsTest extends MRUnitSerialization {

  val data = Bytes.toBytes("d")
  val geom = Bytes.toBytes("geom")

  val key = new LongWritable()
  //  val mapReduceDriver = mapSideRasterizer


  @Test
  def mapSideTest(): Unit = {
    executeMR(mapReduceDriver)
  }

  //
  //  @Test
  //  def reduceSideTest(): Unit = {
  //    executeMR(reduceSideRasterizer)
  //  }


  def executeMR(mapReduceDriver: MapReduceDriver[LongWritable, WayWritable, _, _, _, _]): Unit = {

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


  def mapReduceDriver: MapReduceDriver[LongWritable, WayWritable, WebTileWritable, DoubleWritable, WebTileWritable, Mutation] = {

    val mapReduceDriver =
      MapReduceDriver.newMapReduceDriver.asInstanceOf[MapReduceDriver[LongWritable, WayWritable, WebTileWritable, DoubleWritable, WebTileWritable, Mutation]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set("valueKey", "highway")
    mapReduceDriver.setMapper(new WayTileStats.WayTileMapper)
    mapReduceDriver.setReducer(new WayTileStats.DistanceReducer)
    mapReduceDriver
  }


}
