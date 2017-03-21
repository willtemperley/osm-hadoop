package org.roadlessforest.osm

import java.lang.Iterable

import com.esri.core.geometry._
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableReducer}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.openstreetmap.osmosis.hbase.mr.analysis.TileStatsSerDe
import org.openstreetmap.osmosis.hbase.xyz.WebTileWritable
import org.roadlessforest.osm.grid._
import org.roadlessforest.osm.writable.WayWritable
import xyz.GeometryUtils
import xyz.mercator.MercatorTileCalculator

import scala.collection.JavaConversions._

/*
*
*/
object WayTileStats extends Configured with Tool {

  val width: Int = 43200
  val height: Int = 21600

  val valueKey = "valueKey"

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), WayTileStats, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    //fixme -- add tag??
    if (args.length != 3) {
      println("Usage: WayTileStats input-seqfile-path table [FIXME]")
      return 1
    }

    val conf = getConf
    conf.set(valueKey, args(2))

    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[WayTileMapper])
    job.setReducerClass(classOf[DistanceReducer])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])

    job.setMapOutputKeyClass(classOf[CoordinateWritable])
    job.setMapOutputValueClass(classOf[IntWritable])

    //    job.setOutputKeyClass(classOf[CoordinateWritable])
    //    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))

    TableMapReduceUtil.initTableReducerJob(args(1), classOf[DistanceReducer], job)
    //    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    //    FileOutputFormat.setOutputPath(job, new Path(args(1)))


    if (job.waitForCompletion(true)) 0 else 1

  }

  class DistanceReducer extends TableReducer[WebTileWritable, DoubleWritable, WebTileWritable] {

    //fixme hack for key to differentiate between ints and doubles

    override def reduce(key: WebTileWritable, values: Iterable[DoubleWritable],
                        context: Reducer[WebTileWritable, DoubleWritable, WebTileWritable, Mutation]#Context): Unit = {

      val len: Double = values.map(_.get()).sum

      val put = new Put(key.get())
      put.addColumn(TileStatsSerDe.cfD, TileStatsSerDe.distCol, Bytes.toBytes(len))

      context.write(key, put)

    }
  }

  /**
    * Maps tiles to distances
    */
  class WayTileMapper extends Mapper[LongWritable, WayWritable, WebTileWritable, DoubleWritable] {

    var tag: String = _

    override def setup(context: Mapper[LongWritable, WayWritable, WebTileWritable, DoubleWritable]#Context): Unit = {

      tag = context.getConfiguration.get(valueKey)

      if (tag == null || tag.isEmpty) {
        throw new RuntimeException("No filter tag specified.")
      }
      rasterValueKey.set(tag)
    }

    //the field on which to base the raster value
    val geometryKey = new Text("geometry")


    val rasterValueKey = new Text()

    val pixVal = new IntWritable
    val coord = new CoordinateWritable

    val highwayMap: Map[String, Int] = Map(
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

    val wktReader: OperatorImportFromWkt = OperatorImportFromWkt.local()
    val sr: SpatialReference = SpatialReference.create(4326)
    val tileWritable = new WebTileWritable
    val clipOp = OperatorClip.local()

    val doubleWritable = new DoubleWritable()

    override def map(key: LongWritable, value: WayWritable,
                     context: Mapper[LongWritable, WayWritable, WebTileWritable, DoubleWritable]#Context): Unit = {

      val tileCalculator = new MercatorTileCalculator()

      //The string which will be converted to a raster value
      //      val rasterValueString = value.get(rasterValueKey).asInstanceOf[Text].toString
      //
      //      //fixme hackery
      //      if (rasterValueKey.toString.equals("highway")) {
      //        pixVal.set(highwayMap(rasterValueString))
      //      } else {
      //        pixVal.set(1)
      //      }


      val lineString: Text = value.get(geometryKey).asInstanceOf[Text]
      val string = lineString.toString

      val polyLine = wktReader.execute(0, Geometry.Type.Polyline, string, null)


      val env = new Envelope2D()
      polyLine.queryEnvelope2D(env)
      val tiles = tileCalculator.tilesForEnvelope(env, 14)

      for (tile <- tiles) {

        val x: Geometry = clipOp.execute(polyLine, env, sr, null)

        tileWritable.setTile(tile)
        val length = x.asInstanceOf[Polyline].calculateLength2D()
        doubleWritable.set(length)
        context.write(tileWritable, doubleWritable)

    }
  }

  //fixme move to geometryUtils
  def executeIntersect(poly: Geometry, intersectorGeom: Geometry): Iterator[Geometry] = {

    val inGeoms = new SimpleGeometryCursor(intersectorGeom)
    val intersector = new SimpleGeometryCursor(poly)

    val ix: GeometryCursor = OperatorIntersection.local().execute(inGeoms, intersector, sr, null, 4)

    Iterator.continually(ix.next).takeWhile(_ != null)
  }
}


}

