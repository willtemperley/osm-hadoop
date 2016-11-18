package org.roadlessforest.osm

import java.awt.JobAttributes.DestinationType
import java.awt.Point
import java.awt.image._
import java.io.{File, IOException}
import javax.imageio.{ImageTypeSpecifier, ImageWriteParam}

import org.apache.commons.imaging._
import org.apache.commons.imaging.common.SimpleBufferedImageFactory
import org.apache.commons.imaging.formats.tiff.constants.TiffConstants
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.gce.geotiff.{GeoTiffFormat, GeoTiffWriteParams, GeoTiffWriter}
import org.geotools.geometry.Envelope2D
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.jaitools.imageutils.ImageDataType
import org.opengis.parameter.{GeneralParameterValue, ParameterValueGroup}
import org.roadlessforest.osm.grid.GlobalGrid
import sun.awt.image.ShortInterleavedRaster

/**
  * Created by willtemperley@gmail.com on 17-Nov-16.
  */
object RasterSize {

  val xres = 65536 / 2
  val grid = new GlobalGrid(xres, xres / 2)
  private val simpleBufferedImageFactory: SimpleBufferedImageFactory = new SimpleBufferedImageFactory


  def main(args: Array[String]): Unit = {

//    val colorSpace = ColorSpace.getInstance(ColorSpace.CS_GRAY);

//    val bigImage = new BufferedImage(ColorModel.getRGBdefault, new ShortWritableRaster(grid.w, grid.h), true, null)
//    val bigImage = simpleBufferedImageFactory.getGrayscaleBufferedImage(grid.w, grid.h, false)

    val bigImage = new BufferedImage(grid.w, grid.h, BufferedImage.TYPE_USHORT_GRAY)

    val ras = bigImage.getRaster

    var z: Short = 9999
    for (i <- 0 until grid.w) {
      for (j <- 0 until grid.h) {
        ras.setDataElements(i, j, Array(z))
      }
    }

//    bigImage.setRGB()

    val dataBuffer = bigImage.getData().getDataBuffer.asInstanceOf[DataBufferUShort]
    val y = dataBuffer.getData


//    for (i <- 0 until y.size) {
//      y(i) = 5000
//    }
//    RenderedImage

//    var colorModel = new DirectColorModel()
//    //    val bigImage: SimpleRaster = new SimpleRaster(grid.w, grid.h)
//    val bigImage = new BufferedImage(grid.w, grid.h, BufferedImage.TYPE_BYTE_BINARY, colorModel)
//
    write("e:/tmp/ras/5kffs.tif", bigImage)


  }

  def write(outputImgFN: String, bigImage: RenderedImage): Unit = {

    //getting the write parameters
    val wp = new GeoTiffWriteParams
    val format = new GeoTiffFormat

    //setting compression to LZW
    wp.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    wp.setDestinationType(ImageTypeSpecifier.createGrayscale(16, DataBuffer.TYPE_USHORT, false))
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
