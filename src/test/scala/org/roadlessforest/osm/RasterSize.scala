package org.roadlessforest.osm

import java.awt.image.BufferedImage

import org.roadlessforest.osm.grid.GlobalGrid

/**
  * Created by willtemperley@gmail.com on 17-Nov-16.
  */
object RasterSize {

  val xres = 65536
  val grid = new GlobalGrid(xres, xres / 2)


  def main(args: Array[String]): Unit = {

    val bigImage: BufferedImage = new BufferedImage(grid.w, grid.h, BufferedImage.TYPE_INT_ARGB)

  }

}
