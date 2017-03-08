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
import xyz.tms.TmsTileCalculator

import scala.collection.JavaConversions._

object RoadlessRasterizeMapSide extends Configured with Tool {

  val valueKey = "valueKey"

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), RoadlessRoadCount, args)
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

    val tileWritable = new ImmutableBytesWritable
    val spatialReference = SpatialReference.create(4326)
    val bitsetWritable = new ImmutableBytesWritable
    val zoomLevel = 13
    val bufferDistance = 0.008333

    override def map(key: LongWritable, value: WayWritable,
                     context: Mapper[LongWritable, WayWritable, ImmutableBytesWritable, ImmutableBytesWritable]#Context): Unit = {

      //The string which will be converted to a raster value
      val rasterValueString = value.get(rasterValueKey).asInstanceOf[Text].toString

      val lineString: Text = value.get(geometryKey).asInstanceOf[Text]
      val geometry = OperatorImportFromWkt.local().execute(0, Geometry.Type.Polyline, lineString.toString, null)

      val outputGeom = OperatorBuffer.local.execute(geometry, spatialReference, bufferDistance, null)

      val env = new Envelope2D()
      outputGeom.queryEnvelope2D(env)

      val spatialRef: SpatialReference = SpatialReference.create(4326)

      val tiles = TmsTileCalculator.tilesForEnvelope(env, zoomLevel)
      for (tile <- tiles) {
        val envelopeAsPolygon = tile.getEnvelopeAsPolygon
        val tileIntersects = OperatorIntersects.local().execute(envelopeAsPolygon, outputGeom, spatialRef, null)
        if (tileIntersects) {

          /*
           * Binary encode the tile
           */
          val tileRasterizer = new TileRasterizer(tile, new BinaryScanCallback(256, 256))
          tileWritable.set(tile.encode())
          tileRasterizer.rasterizePolygon(outputGeom.asInstanceOf[Polygon])
          val bits = tileRasterizer.getBitset
          val compressedbytes = Snappy.compress(bits)
          bitsetWritable.set(compressedbytes)
          context.write(tileWritable, bitsetWritable)

        }
      }
    }
  }

  class RasterizedTileStack extends TableReducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable] {

    val outVal = new IntWritable()
    var classToPrecedenceMap: Map[Int, Int] = ConfigurationFactory.getPrecedence

    override def reduce(key: ImmutableBytesWritable, values: Iterable[ImmutableBytesWritable],
                        context: Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation]#Context): Unit = {


      val bitsetCompositor = new BitsetCompositor(256 * 256)

      /*
      Iterate all geometries,
       */
      for (value <- values) {

        val bytes = value.get()
        val v = Snappy.uncompress(bytes)
        bitsetCompositor.or(v)
      }

      val image = BinaryImage.getImage(bitsetCompositor.getBitset, 256, 256)

      val put = new Put(key.get())
      put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("i"), image)
      context.write(key, put)

      writeDebugTile(key, image)

    }

    protected def writeDebugTile(key: ImmutableBytesWritable, bytes: Array[Byte]): Unit = {
      //no-op, can be overridden for debug purposes
    }

//    @throws[IOException]
//    private def writeDebugTile(tile: TileCalculator.TmsTile, bytes: Array[Byte]) {
//      val f: File = new File("e:/tmp/ras/mr-" + tile.toString + ".png")
//      val fileOutputStream: FileOutputStream = new FileOutputStream(f)
//      for (aByte <- bytes) {
//        fileOutputStream.write(aByte)
//      }
//    }

  }

  class RasterizedTileStack2 extends Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable] {

    var classToPrecedenceMap: Map[Int, Int] = ConfigurationFactory.getPrecedence

    val value = new ImmutableBytesWritable()

    override def reduce(key: ImmutableBytesWritable, values: Iterable[ImmutableBytesWritable],
                        context: Reducer[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable]#Context): Unit = {


      val bitsetCompositor = new BitsetCompositor(256 * 256)

      /*
      Iterate all geometries,
       */
      for (value <- values) {

        val bytes = value.get()
        val v = Snappy.uncompress(bytes)
        bitsetCompositor.or(v)
      }

      val image = BinaryImage.getImage(bitsetCompositor.getBitset, 256, 256)

      //key passed through
      value.set(image)
      context.write(key, value)


      writeDebugTile(key, image)

    }

    protected def writeDebugTile(key: ImmutableBytesWritable, bytes: Array[Byte]): Unit = {
      //no-op, can be overridden for debug purposes
    }

  }

}

