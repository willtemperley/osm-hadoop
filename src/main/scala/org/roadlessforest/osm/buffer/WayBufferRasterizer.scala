package org.roadlessforest.osm.buffer

import java.lang.Iterable

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import scala.collection.JavaConversions._

/*
*
*/
object WayBufferRasterizer extends Configured with Tool {

//  val width: Int = 43200
//  val height: Int = 21600

  val width = 32768
  val height = width / 2

  val valueKey = "valueKey"

  def main(args: Array[String]) {

    val res = ToolRunner.run(new Configuration(), WayBufferRasterizer, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    if (args.length != 3) {
      println("Usage: WayBufferRasterizer input-seqfile-path output-seqfile-path tag")
      return 1
    }

    val conf = getConf
    conf.set(valueKey, args(2))

    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[RoadlessRasterizeMapSide.WayRasterMapper])
    job.setReducerClass(classOf[RoadlessRasterizeMapSide.RasterizedTileStack2])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[ImmutableBytesWritable])

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[ImmutableBytesWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0 else 1

  }


}

