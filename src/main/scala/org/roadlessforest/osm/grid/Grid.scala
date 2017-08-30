package org.roadlessforest.osm.grid
import com.esri.core.geometry.Envelope2D
import com.vividsolutions.jts.geom.Coordinate
import org.roadlessforest.tiff.GeoTiffReader.ImageMetadata

/**
  * Created by willtemperley@gmail.com on 20-Jul-17.
  */
class Grid(imageMetadata: ImageMetadata) extends GridDef{

  val env: Envelope2D = imageMetadata.getEnvelope2D

  val w: Int = imageMetadata.getWidth
  val h: Int = imageMetadata.getHeight

  override def snap(coordinate: Coordinate): Coord = {

    val x = coordinate.getOrdinate(0)
    val y = coordinate.getOrdinate(1)

    val imgWidthGeo = env.getWidth
    val imageHeightGeo = env.getHeight

    val xOrig = env.xmin
    val yOrig = env.ymin

    val x1 = (((x - xOrig) * w) / imgWidthGeo).toInt
    val y1 = (((y - yOrig) * h) / imageHeightGeo).toInt

    new Coord((x1, y1))
  }

}
