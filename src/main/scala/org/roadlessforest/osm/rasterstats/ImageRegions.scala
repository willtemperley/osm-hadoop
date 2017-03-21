package org.roadlessforest.osm.rasterstats

import java.io.{File, FileOutputStream}
import java.lang.Iterable

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayPrimitiveWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.coverage.grid.GridCoverage2D
import org.roadlessforest.osm.grid.MercatorTileWritable
import org.roadlessforest.tiff.GeoTiffReader
import org.roadlessforest.xyz.{ImageTileWritable, TileKeyWritable}
import xyz.mercator.{MercatorTile, MercatorTileCalculator}
import xyz.wgs84.TileKey

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 17-Feb-17.
  */
object ImageRegions extends Configured with Tool {

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), ImageRegions, args)
    System.exit(res)
  }

//  val tmplocationKey = "tmpLocationKey"

  override def run(args: Array[String]): Int = {

    if (args.length != 2) {
      println("Usage: ImageRegions input-seqfile-path output-seqfile-path")
      return 1
    }
//    val tmpLocation = args(3)

    val conf = getConf
    /**
      * Very irritatingly Geotools won't read byte arrays directly.
      * So we need to save it to a path.
      */
//    conf.set(tmplocationKey, tmpLocation)

    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[GeotiffMapper])

    job.setReducerClass(classOf[TileReducer])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])
    job.setMapOutputKeyClass(classOf[MercatorTileWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    /**
      * These are nodes and ways, order doesn't matter
      */
    FileInputFormat.addInputPath(job, new Path(args(0)))

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0 else 1
  }

  /**
    * Identity mapper
    */
  class GeotiffMapper extends Mapper[TileKeyWritable, ImageTileWritable, MercatorTileWritable, IntWritable] {

    val tileWritable = new MercatorTileWritable
    val intWritable = new IntWritable

    val mercatorTileCalculator = new MercatorTileCalculator

    override def map(key: TileKeyWritable, value: ImageTileWritable, context: Mapper[TileKeyWritable, ImageTileWritable, MercatorTileWritable, IntWritable]#Context): Unit = {

      val tileKey = new TileKey
      key.readTile(tileKey)

      val imgData = value.getImage
      val w = tileKey.getWidth
      val h = tileKey.getHeight

      val env = tileKey.getEnvelope2D
      val yTop = env.ymax
      val xLeft = env.xmin

      val pixelSizeX = tileKey.getPixelSizeX//(env.getMaxX - xLeft) / w
      val pixelSizeY = tileKey.getPixelSizeY //(yTop - env.getMinY) / h

      val theTile = new MercatorTile()

      var y = yTop
      var offsetLeft = 0
      for (i <- 0 until h) {

//        data.getPixels(0, i, w, 1, arr)
        val arr = imgData.slice(offsetLeft, offsetLeft + w)
        var x = xLeft

        for (pixVal <- arr) {
          x += pixelSizeX

          mercatorTileCalculator.tileForCoordinate(x, y, 14, theTile)

          tileWritable.setTile(theTile)
          intWritable.set(pixVal)
          context.write(tileWritable, intWritable)
        }

        y -= pixelSizeY
        offsetLeft += w
      }
    }
  }

  class TileReducer extends Reducer[MercatorTileWritable, IntWritable, MercatorTileWritable, Text] {

    val nBins = 110

    val keyOut = new MercatorTileWritable()
    val valOut = new Text()

    override def reduce(key: MercatorTileWritable, values: Iterable[IntWritable],
                        context: Reducer[MercatorTileWritable, IntWritable, MercatorTileWritable, Text]#Context): Unit = {

      val intCounter: Array[Int] = new Array[Int](nBins)

      val tile = new MercatorTile()
      key.getTile(tile)

      for (x: IntWritable <- values) {
        intCounter(x.get()) += 1
      }

//      val keyText = tile.toString //Key out
//      keyOut.set(keyText)
      val outTextTemplate = "%s:%s"

      for (i <- intCounter.indices) {
        val count = intCounter(i)
        if (count > 0) {
          val outText = outTextTemplate.format(i, count)
          valOut.set(outText)
//          println(keyText)
//          println(outText)
          context.write(key, valOut)
        }
      }

    }
  }
}
