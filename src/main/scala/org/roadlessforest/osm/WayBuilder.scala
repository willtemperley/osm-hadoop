package org.roadlessforest.osm

import java.lang.Iterable

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.openstreetmap.osmosis.hbase.common.EntityDataAccess
import org.openstreetmap.osmosis.hbase.mr.WayMapper
import org.roadlessforest.osm.config.ConfigurationFactory
import org.roadlessforest.osm.writable.{OsmEntityWritable, ReferencedWayNodeWritable, WayWritable}

import scala.collection.JavaConversions._

/**
  * Takes a WayWritable, joins the referenced way-nodes together
  * and adds a geometry to it in WKT
  *
  * Created by willtemperley@gmail.com on 07-Mar-16.
  */
object WayBuilder extends Configured with Tool {

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

    job.setReducerClass(classOf[WayReducer])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_,_]])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[OsmEntityWritable])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[WayWritable])

    /**
      * These are nodes and ways, order doesn't matter
      */
    FileInputFormat.addInputPath(job, new Path(args(0)))

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_,_]])
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0 else 1
  }

  /**
    * Identity mapper
    */
  class WayMapper extends Mapper[LongWritable, OsmEntityWritable, LongWritable, OsmEntityWritable]

  class WayReducer extends Reducer[LongWritable, OsmEntityWritable, LongWritable, WayWritable] {

    val gf = new GeometryFactory(new PrecisionModel())
    val wktWriter = new WKTWriter
    val wkt = new Text


    override def reduce(key: LongWritable, values: Iterable[OsmEntityWritable],
                        context: Reducer[LongWritable, OsmEntityWritable, LongWritable, WayWritable]#Context): Unit = {

      val(ways, nodes) =  values.map(_.get()).partition(_.isInstanceOf[WayWritable])

      if (ways.size != 1) {
        throw new RuntimeException("Expected a single Way. Actual number found: " + ways.size)
      }

      val wayWritable = ways.head.asInstanceOf[WayWritable]

      val nodeList = nodes.map(_.asInstanceOf[ReferencedWayNodeWritable]).toList.sortBy(_.ordinal)

      val coords: Array[Coordinate] = nodeList.map(f => new Coordinate(f.x, f.y)).toArray

      if (coords.length > 1) {

        val geom = gf.createLineString(coords)
        val out = wktWriter.write(geom)
        wkt.set(out)
        wayWritable.put(WayWritable.geometry, wkt)
        context.write(key, wayWritable)
      }

    }
  }

}
