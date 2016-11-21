package org.roadlessforest.osm.buffer

import java.io.{File, FileOutputStream, IOException}
import java.lang.Iterable

import com.esri.core.geometry._
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableReducer}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.roadlessforest.osm.config.ConfigurationFactory
import org.roadlessforest.osm.grid._
import org.roadlessforest.osm.writable.WayWritable
import xyz.TileCalculator

import scala.collection.JavaConversions._

/*
*
*/
object RoadlessRoadCount extends Configured with Tool {

//  val width: Int = 43200
//  val height: Int = 21600
//  val tileSize: Int = 1080
  val valueKey = "valueKey"

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), RoadlessRoadCount, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    if (args.length != 3) {
      println("Usage: RoadlessRoadCount input-seqfile-path out-table-name tag")
      return 1
    }

    val conf = getConf
    conf.set(valueKey, args(2))

    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[WayMapper])
    job.setReducerClass(classOf[BufferReducer])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[IntWritable])


    FileInputFormat.addInputPath(job, new Path(args(0)))

    //Reduces
    TableMapReduceUtil.initTableReducerJob("buff", classOf[BufferReducer], job)

    if (job.waitForCompletion(true)) 0 else 1
  }

  /**
    *
    */
  class WayMapper extends Mapper[LongWritable, WayWritable, ImmutableBytesWritable, IntWritable] {

    var tag: String = _

    override def setup(context: Mapper[LongWritable, WayWritable, ImmutableBytesWritable, IntWritable]#Context): Unit = {

      tag = context.getConfiguration.get(valueKey)

      if (tag == null || tag.isEmpty) {
        throw new RuntimeException("No filter tag specified.")
      }
      rasterValueKey.set(tag)
    }

    //the field on which to base the raster value
    val geometryKey = new Text("geometry")
    var zoomLevel = 16

    val wkt = new WKTReader

//    val grid = new GlobalGrid(width, height)

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

    val tileWritable = new ImmutableBytesWritable
    val intWritable = new IntWritable(1)

    override def map(key: LongWritable, value: WayWritable,
                     context: Mapper[LongWritable, WayWritable, ImmutableBytesWritable, IntWritable]#Context): Unit = {

      //The string which will be converted to a raster value
      val rasterValueString = value.get(rasterValueKey).asInstanceOf[Text].toString

      //fixme hackery
      if (rasterValueKey.toString.equals("highway")) {
        pixVal.set(highwayMap(rasterValueString))
      } else {
        pixVal.set(1)
      }

      val lineString: Text = value.get(geometryKey).asInstanceOf[Text]
      val geometry = OperatorImportFromWkt.local().execute(0, Geometry.Type.Polyline, lineString.toString, null)

      val env = new Envelope2D()
      geometry.queryEnvelope2D(env)

      val spatialRef: SpatialReference = SpatialReference.create(4326)

      val tiles  = TileCalculator.tilesForEnvelope(env, zoomLevel)
      import scala.collection.JavaConversions._
      for (tile <- tiles) {
        val envelopeAsPolygon = tile.getEnvelopeAsPolygon
        val tileIntersects = OperatorIntersects.local().execute(envelopeAsPolygon, geometry, spatialRef, null)
        if (tileIntersects) {

          /*
           * Binary encode the tile
           */
          tileWritable.set(tile.encode())
          context.write(tileWritable, intWritable)
        }
      }
    }
  }

  //
  class BufferReducer extends TableReducer[ImmutableBytesWritable, IntWritable, ImmutableBytesWritable] {

    val outVal = new IntWritable()
    var classToPrecedenceMap: Map[Int, Int] = ConfigurationFactory.getPrecedence
    val spatialReference = SpatialReference.create(4326)

    override def reduce(key: ImmutableBytesWritable, values: Iterable[IntWritable],
                        context: Reducer[ImmutableBytesWritable, IntWritable, ImmutableBytesWritable, Mutation]#Context): Unit = {

      //TODO: process /user/tempehu/osm/highways
      //TODO: /user/tempehu/osm/highways
      //TODO: translate unit tests

//      val tile = TileCalculator.decodeTile(key.get())
//      val tileRasterizer = new TileRasterizer(tile, new BinaryScanCallback(256, 256))
//
//      /*
//      Iterate all geometries,
//       */
//      for (value <- values) {
//        val geometry: Geometry = OperatorImportFromWkt.local.execute(0, Geometry.Type.Polyline, value.toString, null)
//        val outputGeom = OperatorBuffer.local.execute(geometry, spatialReference, 0.08333, null)
//        tileRasterizer.rasterizePolygon(outputGeom.asInstanceOf[Polygon])
//      }
//
//      val image: Array[Byte] = tileRasterizer.getImage
////      val put = createTileImagePut(key, image)
//
      val count = values.size
      val put = new Put(key.get())
      put.addColumn(TileDataAcess.cf, TileDataAcess.roadCount, Bytes.toBytes(count))
      context.write(key, put)
//
////      writeDebugTile(tile, image)
//
//    }
//
//    @throws[IOException]
//    private def writeDebugTile(tile: TileCalculator.Tile, bytes: Array[Byte]) {
//      val f: File = new File("e:/tmp/ras/mr-" + tile.toString + ".png")
//      val fileOutputStream: FileOutputStream = new FileOutputStream(f)
//      for (aByte <- bytes) {
//        fileOutputStream.write(aByte)
//      }
    }

  }

//  def createTileImage(ImmutableBytesWritable key, byte[] imgData) throws IOException {
//    KeyValue kv = new KeyValue(key.get(), EntityDataAccess.data, imgData);
//    put.add(kv);
//    return put;

}

