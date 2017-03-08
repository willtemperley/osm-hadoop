package org.roadlessforest.osm.rasterstats

import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.lang.Iterable
import javax.imageio.ImageIO
import javax.imageio.stream.ImageInputStream

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableReducer
import org.apache.hadoop.io.{ArrayPrimitiveWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.GridFormatFinder
import org.geotools.gce.geotiff.GeoTiffReader
import org.roadlessforest.osm.grid.MercatorTileWritable
import org.roadlessforest.osm.writable.{OsmEntityWritable, WayWritable}
import org.roadlessforest.tiff.WriteParams
import xyz.mercator.{GlobalMercator, MercatorTile, MercatorTileCalculator}

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 17-Feb-17.
  */
object ImageRegions extends Configured with Tool {

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), ImageRegions, args)
    System.exit(res)
  }

  val tmplocationKey = "tmpLocationKey"

  override def run(args: Array[String]): Int = {

    if (args.length != 3) {
      println("Usage: WayBuilder input-seqfile-path output-seqfile-path tmpfile-location")
      return 1
    }
    val tmpLocation = args(3)

    val conf = getConf
    /**
      * Very irritatingly Geotools won't read byte arrays directly.
      * So we need to save it to a path.
      */
    conf.set(tmplocationKey, tmpLocation)

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
  class GeotiffMapper extends Mapper[Text, ArrayPrimitiveWritable, MercatorTileWritable, IntWritable] {

    val tileWritable = new MercatorTileWritable
    val intWritable = new IntWritable
    var tmpLoc = "/tmp/"

    override def setup(context: Mapper[Text, ArrayPrimitiveWritable, MercatorTileWritable, IntWritable]#Context): Unit = {
      tmpLoc = context.getConfiguration.get(tmplocationKey)
    }

    def get(bytes: Array[Byte]): GridCoverage2D = {

//      val in = new ByteArrayInputStream(bytes);
//      BufferedImage bImageFromConvert = ImageIO.read(in);

//      val bais = new ByteArrayInputStream(bytes);
//
//      val iis = ImageIO.createImageInputStream(bais);

      //fixme Why can't I read a byte array direct??
      //fixme do I really need to implement an image
      val f = File.createTempFile("asdf", ".tif", new File(tmpLoc))

      val outputStream = new FileOutputStream(f)
      outputStream.write(bytes)

      val gridReader = new GeoTiffReader(f, WriteParams.longitudeFirst)

      val gridCoverage = gridReader.read(null)
      gridCoverage
    }

    val mercatorTileCalculator = new MercatorTileCalculator

    override def map(key: Text, value: ArrayPrimitiveWritable, context: Mapper[Text, ArrayPrimitiveWritable, MercatorTileWritable, IntWritable]#Context): Unit = {

      println("Record input key: " + key.toString)
      val bytes = value.get.asInstanceOf[Array[Byte]]

      val coverage = get(bytes)

      val env = coverage.getEnvelope2D

      val img = coverage.getRenderedImage
      val w = img.getWidth
      val h = img.getHeight
      val data = img.getData()

      val yTop = env.getMaxY
      val xLeft = env.getMinX

      val pixelSizeX = (env.getMaxX - xLeft) / w
      val pixelSizeY = (yTop - env.getMinY) / h

      val theTile = new MercatorTile()

      var y = yTop
      for (i <- 0 until h) {

        val arr = new Array[Int](w)
        data.getPixels(0, i, w, 1, arr)
        var x = xLeft

        for (pixVal <- arr) {
          x += pixelSizeX

          mercatorTileCalculator.tileForCoordinate(x, y, 14, theTile)

          tileWritable.setTile(theTile)
          intWritable.set(pixVal)
          context.write(tileWritable, intWritable)
        }

        y -= pixelSizeY
      }
    }
  }

  class TileReducer extends Reducer[MercatorTileWritable, IntWritable, Text, Text] {

    val nBins = 110

    val keyOut = new Text()
    val valOut = new Text()

    override def reduce(key: MercatorTileWritable, values: Iterable[IntWritable],
                        context: Reducer[MercatorTileWritable, IntWritable, Text, Text]#Context): Unit = {

      val intCounter: Array[Int] = new Array[Int](nBins)


      val tile = new MercatorTile()
      key.getTile(tile)

      for (x: IntWritable <- values) {
        intCounter(x.get()) += 1
      }

      val keyText = tile.toString //Key out
      keyOut.set(keyText)
      val outTextTemplate = "%s:%s"

      for (i <- intCounter.indices) {
        val count = intCounter(i)
        if (count > 0) {
          val outText = outTextTemplate.format(i, count)
          valOut.set(outText)
//          println(keyText)
//          println(outText)
          context.write(keyOut, valOut)
        }
      }

    }
  }
}
