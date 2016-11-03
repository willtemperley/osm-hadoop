package org.roadlessforest.osm

import java.lang.Iterable

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.openstreetmap.osmosis.hbase.common.EntityDataAccess
import org.openstreetmap.osmosis.hbase.mr.WayMapper
import org.roadlessforest.osm.WayBuilder.WayReducer
import org.roadlessforest.osm.config.ConfigurationFactory
import org.roadlessforest.osm.writable.{OsmEntityWritable, ReferencedWayNodeWritable, WayWritable}

import scala.collection.JavaConversions._

/**
  * Takes a WayWritable, joins the referenced way-nodes together
  * and adds a geometry to it in WKT
  *
  * Created by willtemperley@gmail.com on 07-Mar-16.
  */
object WayLoader extends Configured with Tool {

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), WayBuilder, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    if (args.length != 2) {
      println("Usage: WayBuilder input-seqfile-path output-seqfile-path")
      return 1
    }

    val conf = getConf
    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[WayMapper])

    job.setReducerClass(classOf[HBaseWayReducer])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_,_]])
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])

    val outTable = new HTable(conf, "way_geom")

    /**
      * These are nodes and ways, order doesn't matter
      */
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    HFileOutputFormat2.configureIncrementalLoad(job, outTable, outTable.getRegionLocator)

    if (job.waitForCompletion(true)) 0 else 1

  }

  /**
    * Identity mapper
    */
/*  class WayMapper extends Mapper[LongWritable, OsmEntityWritable, ImmutableBytesWritable, Cell] {

    override def map(key: LongWritable, value: OsmEntityWritable, context: Mapper[LongWritable, OsmEntityWritable, ImmutableBytesWritable, Cell]#Context): Unit = {
      hbaseKey.set(k)
      val cell = new KeyValue(hbaseKey.get(), EntityDataAccess.data, Bytes.toBytes("geometry"), out)
    }
  }*/

  class HBaseWayReducer extends Reducer[LongWritable, OsmEntityWritable, ImmutableBytesWritable, Cell] {

    val gf = new GeometryFactory(new PrecisionModel())
    val wkbWriter = new WKBWriter()
    val hbaseKey = new ImmutableBytesWritable()

    override def reduce(key: LongWritable, values: Iterable[OsmEntityWritable],
                        context: Reducer[LongWritable, OsmEntityWritable, ImmutableBytesWritable, Cell]#Context): Unit = {

      val(ways, nodes) =  values.map(_.get()).partition(_.isInstanceOf[WayWritable])

      if (ways.size != 1) {
        throw new RuntimeException("Expected a single Way. Actual number found: " + ways.size)
      }

      val nodeList = nodes.map(_.asInstanceOf[ReferencedWayNodeWritable]).toList.sortBy(_.ordinal)

      val coords: Array[Coordinate] = nodeList.map(f => new Coordinate(f.x, f.y)).toArray

      if (coords.length > 1) {

        val geom = gf.createLineString(coords)
        val out = wkbWriter.write(geom)

        val k = Bytes.toBytes(key.get())

        hbaseKey.set(k)
        val cell = new KeyValue(hbaseKey.get(), EntityDataAccess.data, Bytes.toBytes("geometry"), out)

        context.write(hbaseKey, cell)
      }

    }

  }

}
