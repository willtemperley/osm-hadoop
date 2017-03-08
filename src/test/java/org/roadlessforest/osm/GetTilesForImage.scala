package org.roadlessforest.osm

import java.io.File
import java.net.URL
import java.util

import com.esri.core.geometry.Envelope2D
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.geotools.gce.geotiff.GeoTiffReader
import org.roadlessforest.osm.shp.{GeomType, ShapeWriter}
import xyz.mercator.{MercatorTile, MercatorTileCalculator}

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 28-Feb-17.
  */
object GetTilesForImage {

  val geomFact = new GeometryFactory()

  def getImageEnv(resourceURL: URL): Envelope2D = {

    val gridReader = new GeoTiffReader(resourceURL)
    val gridCoverage = gridReader.read(null)
    val imgEnv = gridCoverage.getEnvelope2D
    val env = new Envelope2D
    env.setCoords(imgEnv.getMinX, imgEnv.getMinY, imgEnv.getMaxX, imgEnv.getMaxY)
    env
  }

  val folder = "E:\\ROADLESS\\Classifv11\\"
  val tiff = "Classif33y_AFRWest_UL18W14N_LR7E4S-0000000000-0000065536.tif"

  def main(args: Array[String]): Unit = {

//    val resourceURL: URL = this.getClass.getClassLoader.getResource("data/littletiff.tif")
    val resourceURL = new File(folder, tiff).toURL
    val env = getImageEnv(resourceURL)
//    val env = new Envelope2D
//    env.setCoords(28.67, -3.42, 29.09, -2.94)

    val calculator = new MercatorTileCalculator()
    val tiles: util.List[MercatorTile] = calculator.tilesForEnvelope(env, 14)
//    var globalMercator = new GlobalMercator

    val sw = new ShapeWriter(GeomType.Polygon)

    for (tile <- tiles) {

      val tileEnv: com.esri.core.geometry.Envelope2D = calculator.getTileEnvelope(tile)

      val ll = new Coordinate(tileEnv.xmin, tileEnv.ymin)
      val tl = new Coordinate(tileEnv.xmin, tileEnv.ymax)
      val tr = new Coordinate(tileEnv.xmax, tileEnv.ymax)
      val br = new Coordinate(tileEnv.xmax, tileEnv.ymin)

      val poly = geomFact.createPolygon(Array(ll, tl, tr, br, ll))

      sw.addFeature(poly, Seq(tile.toString))

      println(tile)
    }

    sw.write("e:/tmp/completeness/tiles.shp")
  }

}
