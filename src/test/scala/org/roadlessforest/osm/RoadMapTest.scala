package org.roadlessforest.osm

import java.awt.image.BufferedImage
import java.io.{File, FileInputStream}
import java.util
import javax.imageio.ImageWriteParam

import com.esri.core.geometry.examples.ShapefileGeometryCursor
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mrunit.types
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.gce.geotiff.{GeoTiffFormat, GeoTiffWriteParams, GeoTiffWriter}
import org.geotools.geometry.Envelope2D
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.parameter.{GeneralParameterValue, ParameterValueGroup}
import org.openstreetmap.osmosis.hbase.xyz.geotiff.GeoTiffReader
import org.roadlessforest.osm.grid.CoordinateWritable

import scala.collection.mutable

//import com.esri.core.geometry.examples.ShapefileGeometryCursor
import com.esri.core.geometry.OperatorExportToWkt
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.junit.Test
import org.roadlessforest.osm.writable.WayWritable

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 14-Nov-16.
  */
class RoadMapTest extends MRUnitSerialization {

  val data = Bytes.toBytes("d")
  val geom = Bytes.toBytes("geom")

  val key = new LongWritable()
  //  val mapReduceDriver = mapSideRasterizer

  val w = 100;
  val h = 100;

  @Test
  def exec(): Unit = {

    val geoTiffReader = new GeoTiffReader
    val referencedImage = geoTiffReader.readGeotiffFromFile(new File("src/test/resources/ras/can.tif"))

    val imgMeta: GeoTiffReader.ImageMetadata = referencedImage.getMetadata

    val mapReduceDriver = getMapReduceDriver
    setupSerialization(mapReduceDriver)

    val fileInputStream = new FileInputStream(new File("src/test/resources/shp/can.shp"))
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

    val outputs: util.List[types.Pair[CoordinateWritable, IntWritable]] = mapReduceDriver.run

    for (output <- outputs) {
      print(output)
    }

    val pixelsD: mutable.Seq[(CoordinateWritable, IntWritable)] = outputs.map(f => (f.getFirst, f.getSecond))
    //    val pix//: Array[(CoordinateWritable, Int)] = pixelsD.map(f => (f._1, f._2.get())).collect()
    val bigImage: BufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_GRAY)
    val bigRas = bigImage.getRaster

    def yIdx(y: Int): Int = h - y - 1

    val pixelsC: mutable.Seq[(Int, Int, Int)] = pixelsD
      .map(f => (f._1.indices, f._2.get()))
      .map(g => (g._1._1, yIdx(g._1._2), g._2))

    for ((x, y, z) <- pixelsC) {

      if (x >= 0 && x < w && y >= 0 && y < h) {

        bigRas.setPixel(x, y, Array(z))

      } else {
        println(x, y, z)
      }

    }

  }

  def getMapReduceDriver: MapReduceDriver[LongWritable, WayWritable, CoordinateWritable, IntWritable, CoordinateWritable, IntWritable] = {

    val mapReduceDriver =
      MapReduceDriver.newMapReduceDriver
        .asInstanceOf[MapReduceDriver[LongWritable, WayWritable, CoordinateWritable, IntWritable, CoordinateWritable, IntWritable]]

    setupSerialization(mapReduceDriver)
    mapReduceDriver.getConfiguration.set("valueKey", "highway")
    mapReduceDriver.setMapper(new WayRasterizerAfr.WayRasterMapper)
    mapReduceDriver.setReducer(new WayRasterizerAfr.PixelReducer)
    mapReduceDriver
  }


  def write(outputImgFN: String, bigImage: BufferedImage): Unit = {

    //getting the write parameters
    val wp: GeoTiffWriteParams = new GeoTiffWriteParams
    val format = new GeoTiffFormat

    //setting compression to LZW
    wp.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    wp.setCompressionType("DEFLATE")
    wp.setCompressionQuality(1.0F)

    val params: ParameterValueGroup = format.getWriteParameters
    params.parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName.toString).setValue(wp)
    val x = params.values()

    val bbox = new Envelope2D(DefaultGeographicCRS.WGS84, -180, -90, 360, 180)
    val coverage = new GridCoverageFactory().create("tif", bigImage, bbox)
    val gtw = new GeoTiffWriter(new File(outputImgFN))

    val retainAxes = GeoTiffFormat.RETAIN_AXES_ORDER
    val retainAxesValue = retainAxes.createValue()
    retainAxesValue.setValue(true)

    x.add(retainAxesValue)
    gtw.write(coverage, x.toArray(new Array[GeneralParameterValue](2)))

  }

}
