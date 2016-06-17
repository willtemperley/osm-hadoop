package org.roadlessforest.osm

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageWriteParam

import com.google.common.primitives.{Ints, Longs}
import com.vividsolutions.jts.geom.{Coordinate, LineString}
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.gce.geotiff.{GeoTiffFormat, GeoTiffWriteParams, GeoTiffWriter}
import org.geotools.geometry.Envelope2D
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.parameter.{GeneralParameterValue, ParameterValueGroup}
import org.roadlessforest.osm.grid.{Coord, CoordinateWritable, GlobalGrid}
import org.roadlessforest.osm.raster.{Plotter, Rasterizer}
import org.roadlessforest.osm.writable.WayWritable

import scala.collection.mutable.ListBuffer

/**
  * Created by willtemperley@gmail.com on 04-Mar-16.
  */
object SparkRasterizer {

  val grid = new GlobalGrid(43200, 21600)

  class LinePlotter extends Plotter {

    var value: Int = 0

    var output: ListBuffer[Coord] = new ListBuffer[Coord]

    override def plot(x: Int, y: Int): Unit = {
      output += new Coord((x, y))
    }

  }

  def rasterize(v: Int, geom: LineString): Iterator[(Long, Int)] = {

    val linePlotter = new LinePlotter

    val coords = geom.getCoordinates

    val slide: Iterator[Array[Coordinate]] = coords.sliding(2)
    for (pair <- slide) {

      val a: Coord = grid.snap(pair(0))
      val b: Coord = grid.snap(pair(1))

      Rasterizer.rasterize(a.x, a.y, b.x, b.y, linePlotter)
    }

    linePlotter.output.map(f => (f.asLong, v)).iterator
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rasterizer")
    val sc = new SparkContext(conf)
    val sequenceFileInputPath = args(0)
    val ways = sc.sequenceFile(sequenceFileInputPath, classOf[LongWritable], classOf[WayWritable])

    val valueToGeom = ways.map(_._2).map(f => (f.get(new Text("highway")).toString, f.get(new Text("geometry")).toString))

    val highwayMap = Map(
      "motorway" -> 1,
      "trunk" -> 2,
      "railway" -> 3, //placeholder
      "primary" -> 4,
      "secondary" -> 5,
      "tertiary" -> 6,
      "motorway link" -> 7,
      "primary link" -> 8,
      "unclassified" -> 9,
      "road" -> 10,
      "residential" -> 11,
      "service" -> 12,
      "track" -> 13,
      "pedestrian" -> 14
    ).withDefaultValue(15)

//    val wktReader = new WKTReader()
    val intToGeom: RDD[(Int, LineString)] = valueToGeom.map(f => (highwayMap(f._1), new WKTReader().read(f._2).asInstanceOf[LineString]))

    val x: RDD[(Long, Int)] = intToGeom.flatMap(f => rasterize(f._1, f._2))
//    val y: RDD[(Int, Coord)] = x.map(f => (f._2, f._1))
//    y.red


    val bigImage: BufferedImage = new BufferedImage(grid.w, grid.h, BufferedImage.TYPE_BYTE_GRAY)

    val bigRas = bigImage.getRaster


    def lt(a: Int, b: Int) = if (a < b) a else b
    val unique: RDD[(Long, Int)] = x.reduceByKey(lt)

//    val pixelsD = sc.sequenceFile(sequenceFileInputPath, classOf[IntPairWritable], classOf[IntWritable])
    val pixelsC: Array[(Long, Int)] = unique.collect().map(f => (f._1, f._2))
//      .map(f => (f._1.indices, f._2.get()))
//      .map(g => (g._1._1, yIdx(g._1._2), g._2))
//      .collect()
    def yIdx(y: Int): Int = grid.h - y - 1


    for ((l, z) <- pixelsC) {

      val b = Longs.toByteArray(l)
      val x = Ints.fromByteArray(b.slice(0,4))
      val y = yIdx(Ints.fromByteArray(b.slice(4,8)))

      if (x >= 0 && x < grid.w && y >= 0 && y < grid.h) {

        bigRas.setPixel(x, y, Array(z))

      } else println(x, y, z)

    }

    val outputImgFN = args(1)
    write(outputImgFN, bigImage)

  }

  def write(outputImgFN: String, bigImage: BufferedImage): Unit = {

    //getting the write parameters
    val wp = new GeoTiffWriteParams
    val format = new GeoTiffFormat

    //setting compression to LZW
    wp.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    wp.setCompressionType("LZW")
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
