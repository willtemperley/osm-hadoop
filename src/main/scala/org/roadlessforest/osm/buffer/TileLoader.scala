package org.roadlessforest.osm.buffer

import java.lang.Iterable

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.WKBWriter
import org.apache.commons.lang.ArrayUtils
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.roadlessforest.osm.WayBuilder.WayMapper
import org.roadlessforest.osm.writable.{OsmEntityWritable, ReferencedWayNodeWritable, WayWritable}
import xyz.tms.TmsTile

import scala.collection.JavaConversions._

/**
  * Takes a WayWritable, joins the referenced way-nodes together
  * and adds a geometry to it in WKT
  *
  * Created by willtemperley@gmail.com on 07-Mar-16.
  */
object TileLoader extends Configured with Tool {

  def main(args: Array[String]) {
    val res = ToolRunner.run(new Configuration(), TileLoader, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    if (args.length != 3) {
      println("Usage: TileLoader input-seqfile-path output-hfile-path table-name")
      return 1
    }

    val conf = getConf
    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[TileToCellMapper])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_,_]])
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])

    val outTable = new HTable(conf, args(2))

    /**
      * These are nodes and ways, order doesn't matter
      */
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    HFileOutputFormat2.configureIncrementalLoad(job, outTable, outTable.getRegionLocator)

    if (job.waitForCompletion(true)) 0 else 1

  }

  class TileToCellMapper extends Mapper[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Cell] {

    override def map(key: ImmutableBytesWritable, value: ImmutableBytesWritable,
                     context: Mapper[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Cell]#Context): Unit = {

      val bytes = key.get()
      val keyValue = new KeyValue(bytes, TmsTile.cf, TmsTile.cimg, value.get())
      context.write(key, keyValue)
    }

  }


}
