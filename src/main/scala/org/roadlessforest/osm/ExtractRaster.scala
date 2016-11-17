package org.roadlessforest.osm

import java.awt.image.BufferedImage
import java.io.File
import java.lang.Boolean
import javax.imageio.ImageWriteParam

import org.apache.hadoop.io.IntWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.io.imageio.GeoToolsWriteParams
import org.geotools.gce.geotiff.{GeoTiffFormat, GeoTiffWriteParams, GeoTiffWriter}
import org.geotools.geometry.Envelope2D
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.parameter.{GeneralParameterValue, ParameterValue, ParameterValueGroup}
import org.roadlessforest.osm.grid.{GlobalGrid, CoordinateWritable}

/**
  * Created by willtemperley@gmail.com on 04-Mar-16.
  */
object ExtractRaster {

//  val grid = new GlobalGrid(43200, 21600)
  val xres = 65536
  val grid = new GlobalGrid(xres, xres / 2)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("raster_extract")

    val bigImage: BufferedImage = new BufferedImage(grid.w, grid.h, BufferedImage.TYPE_BYTE_GRAY)

    val bigRas = bigImage.getRaster

    if (conf.get("spark.master", "").isEmpty) {
      conf.setMaster("local[8]")
    }

    val sc = new SparkContext(conf)

    val sequenceFileInputPath = args(0)
    val outputImgFN = args(1)

    def yIdx(y: Int): Int = grid.h - y - 1

    val pixelsD = sc.sequenceFile(sequenceFileInputPath, classOf[CoordinateWritable], classOf[IntWritable])
    val pixelsC = pixelsD
      .map(f => (f._1.indices, f._2.get()))
      .map(g => (g._1._1, yIdx(g._1._2), g._2))
      .collect()

    for ((x,y,z) <- pixelsC) {

      if (x >= 0 && x < grid.w && y >= 0 && y < grid.h) {

        bigRas.setPixel(x, y, Array(z))

      } else println(x, y, z)

    }

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
