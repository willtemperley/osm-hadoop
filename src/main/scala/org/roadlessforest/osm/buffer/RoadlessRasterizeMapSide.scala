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
import org.xerial.snappy.Snappy
import xyz.TileCalculator

import scala.collection.JavaConversions._

object RoadlessRasterizeMapSide extends Configured with Tool {

  val valueKey = "valueKey"

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), RoadlessRasterizeMapSide, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    if (args.length != 3) {
      println("Usage: RoadlessRasterizeMapSide input-seqfile-path out-table-name tag")
      return 1
    }

    val conf = getConf
    conf.set(valueKey, args(2))

    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[WayRasterMapper])
    job.setReducerClass(classOf[RasterizedTileStack])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[ImmutableBytesWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))

    //Reduces
    TableMapReduceUtil.initTableReducerJob("buff", classOf[RasterizedTileStack], job)

    if (job.waitForCompletion(true)) 0 else 1
  }

  /**
    *
    */
  class WayRasterMapper extends Mapper[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable] {

    var tag: String = _

    override def setup(context: Mapper[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable]#Context): Unit = {

      tag = context.getConfiguration.get(valueKey)

      if (tag == null || tag.isEmpty) {
        throw new RuntimeException("No filter tag specified.")
      }
      rasterValueKey.set(tag)
    }

    //the field on which to base the raster value
    val geometryKey = new Text("geometry")

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
    val spatialReference = SpatialReference.create(4326)
    val bitsetWritable = new ImmutableBytesWritable
    var zoomLevel = 10

    override def map(key: LongWritable, value: WayWritable,
                     context: Mapper[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable]#Context): Unit = {

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
      for (tile <- tiles) {
        val envelopeAsPolygon = tile.getEnvelopeAsPolygon
        val tileIntersects = OperatorIntersects.local().execute(envelopeAsPolygon, geometry, spatialRef, null)
        if (tileIntersects) {

          val outputGeom = OperatorBuffer.local.execute(geometry, spatialReference, 0.08333, null)
          /*
           * Binary encode the tile
           */
          val tileRasterizer = new TileRasterizer(tile, new BinaryScanCallback(256, 256))
          tileWritable.set(TileCalculator.encodeTile(tile))
          tileRasterizer.rasterizePolygon(outputGeom.asInstanceOf[Polygon])
          val bits = tileRasterizer.getBitset
          val compressedbytes = Snappy.compress(bits)
          bitsetWritable.set(compressedbytes)
          context.write(tileWritable, bitsetWritable)

        }
      }
    }
  }

  //
  class RasterizedTileStack extends TableReducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable] {

    val outVal = new IntWritable()
    var classToPrecedenceMap: Map[Int, Int] = ConfigurationFactory.getPrecedence

    override def reduce(key: ImmutableBytesWritable, values: Iterable[ImmutableBytesWritable],
                        context: Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation]#Context): Unit = {

      //TODO: process /user/tempehu/osm/highways
      //TODO: /user/tempehu/osm/highways
      //TODO: translate unit tests

      val tile = TileCalculator.decodeTile(key.get())

      val bitsetCompositor = new BitsetCompositor(256 * 256)

      /*
      Iterate all geometries,
       */
      for (value <- values) {

        val bytes = value.get()
        val v = Snappy.uncompress(bytes)
        bitsetCompositor.or(v)
//        val geometry: Geometry = OperatorImportFromWkt.local.execute(0, Geometry.Type.Polyline, value.toString, null)
//        val outputGeom = OperatorBuffer.local.execute(geometry, spatialReference, 0.08333, null)
//        tileRasterizer.rasterizePolygon(outputGeom.asInstanceOf[Polygon])
      }

      val image = BinaryImage.getImage(bitsetCompositor.getBitset, 256, 256)
//      val put = createTileImagePut(key, image)

      val put = new Put(key.get())
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("i"), image)
      context.write(key, put)

//      writeDebugTile(tile, image)

    }

    @throws[IOException]
    private def writeDebugTile(tile: TileCalculator.Tile, bytes: Array[Byte]) {
      val f: File = new File("e:/tmp/ras/mr-" + tile.toString + ".png")
      val fileOutputStream: FileOutputStream = new FileOutputStream(f)
      for (aByte <- bytes) {
        fileOutputStream.write(aByte)
      }
    }

  }

//  def createTileImage(ImmutableBytesWritable key, byte[] imgData) throws IOException {
//    KeyValue kv = new KeyValue(key.get(), EntityDataAccess.data, imgData);
//    put.add(kv);
//    return put;

}

