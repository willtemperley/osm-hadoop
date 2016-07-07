package org.roadlessforest.osm

import java.lang.Iterable

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.roadlessforest.osm.grid._
import org.roadlessforest.osm.raster.{Plotter, Rasterizer}
import org.roadlessforest.osm.writable.WayWritable

import scala.collection.JavaConversions._

/*
*
*/
object WayRasterizer extends Configured with Tool {

  val width: Int = 43200
  val height: Int = 21600
  val tileSize: Int = 1080
  val valueKey = "valueKey"

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), WayRasterizer, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    if (args.length != 3) {
      println("Usage: WayRasterizer input-seqfile-path output-seqfile-path tag")
      return 1
    }

    val conf = getConf
    conf.set(valueKey, args(2))

    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[WayRasterMapper])
    job.setReducerClass(classOf[PixelReducer])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])

    job.setMapOutputKeyClass(classOf[CoordinateWritable])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setOutputKeyClass(classOf[CoordinateWritable])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0 else 1

  }


  /**
    *
    */
  class WayRasterMapper extends Mapper[LongWritable, WayWritable, CoordinateWritable, IntWritable]  {

    var tag: String = _

    override def setup(context: Mapper[LongWritable, WayWritable, CoordinateWritable, IntWritable]#Context): Unit = {

      tag = context.getConfiguration.get(valueKey)

      if (tag == null || tag.isEmpty) {
        throw new RuntimeException("No filter tag specified.")
      }
      rasterValueKey.set(tag)
    }

    //the field on which to base the raster value
    val geometryKey = new Text("geometry")

    val wkt = new WKTReader

    val grid = new GlobalGrid(width, height)

    val rasterValueKey = new Text()

    val pixVal = new IntWritable
    val coord = new CoordinateWritable

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


    override def map(key: LongWritable, value: WayWritable,
                     context: Mapper[LongWritable, WayWritable, CoordinateWritable, IntWritable]#Context): Unit = {

      val wktReader = new WKTReader

      val plotter = new Plotter {


        override def plot(x: Int, y: Int): Unit = {
          coord.set(x, y)
          context.write(coord, pixVal)
        }

      }

      //The string which will be converted to a raster value
      val rasterValueString = value.get(rasterValueKey).asInstanceOf[Text].toString

      //fixme hackery
      if (rasterValueKey.toString.equals("highway")) {
        pixVal.set(highwayMap(rasterValueString))
      } else if (rasterValueKey.toString.equals("navigable")){

      } else {
        pixVal.set(1)
      }

      val lineString: Writable = value.get(geometryKey).asInstanceOf[Text]
      val geom = wktReader.read(lineString.toString)

      if (geom.getLength > 50) {
        System.err.print(key.get() + "==" + geom.getLength)
        return
      }

      val coords = geom.getCoordinates

      //Sliding iterates the gaps between fenceposts :)
      val slide: Iterator[Array[Coordinate]] = coords.sliding(2)
      for (pair <- slide) {

        val a: Coord = grid.snap(pair(0))
        val b: Coord = grid.snap(pair(1))

        Rasterizer.rasterize(a.x, a.y, b.x, b.y, plotter)

      }
    }
  }

  class PixelReducer extends Reducer[CoordinateWritable, IntWritable, CoordinateWritable, IntWritable] {

    val outVal = new IntWritable()

    val classToPrecedenceMap = Map(
      1 -> 1,//"motorway"
      2 -> 1,//"trunk"
      4 -> 2,//"primary"
      5 -> 3,//"secondary"
      6 -> 4,//"tertiary"
      7 -> 1,//"motorway link"
      8 -> 3,//"primary link"
      9 -> 5,//"unclassified"
      10 -> 5,//"road"
      11 -> 6,//"residential"
      12 -> 7,//"service"
      13 -> 5,//"track"
      14 -> 8,//"pedestrian"
      15 -> 9//"Other"
    ).withDefaultValue(9)

    override def reduce(key: CoordinateWritable, values: Iterable[IntWritable],
                        context: Reducer[CoordinateWritable, IntWritable, CoordinateWritable, IntWritable]#Context): Unit = {

      //Need to find the pixel with the lowest value
      val pixelValues = values.map(_.get())

      //Join
//      val pixelsToPrecedence = pixelValues.zip(pixelValues.map(classToPrecedenceMap))
      val precedenceToPixels = pixelValues.map(classToPrecedenceMap).zip(pixelValues)

//      val pixelValue = values.map(_.get()).map(classToPrecedenceMap).min
      val min: (Int, Int) = precedenceToPixels.min
      val x = min._2

      outVal.set(x)
      context.write(key, outVal)
    }
  }

}

