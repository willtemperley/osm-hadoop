package org.roadlessforest.osm

import java.lang.Iterable

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer
import org.openstreetmap.osmosis.core.domain.v0_6._
import org.openstreetmap.osmosis.pbf.PbfBlobDecoder2
import org.roadlessforest.osm.filter.EntityFilters

import scala.collection.JavaConversions._
import java.util

import org.roadlessforest.osm.writable._

/**
  * Created by willtemperley@gmail.com on 07-Mar-16.
  */
object NodeJoiner extends Configured with Tool {

  val FILTER_KEY = "FILTER_KEY"

  def main(args: Array[String]): Unit = {

    val res = ToolRunner.run(new Configuration(), NodeJoiner, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {

    if (args.length != 3) {
      println("Usage: NodeJoiner input-seqfile-path output-seqfile-path tag-key")
      return 1
    }

    val conf = getConf
    conf.set(FILTER_KEY, args(2))
    val job: Job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[OsmEntityMapper])
    job.setReducerClass(classOf[WayNodeReducer])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[_,_]])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[OsmEntityWritable])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[OsmEntityWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_,_]])
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0 else 1
  }

  /**
    * Nodes and way nodes are mapped to the node id, ways mapped to the way id.
    *
    * This mixing node and way ids is messy but efficient - otherwise a second reading  step would be required.
    *
    */
  class OsmEntityMapper extends Mapper[Text, ArrayPrimitiveWritable, LongWritable, OsmEntityWritable]  {

    val k = new LongWritable
    val v = new OsmEntityWritable

    val wn = new WayNodeWritable
    val nn = new NodeWritable

    var tags: Array[String] = _
    var filterByTags: (Entity) => Boolean = _

    override def setup(context: Mapper[Text, ArrayPrimitiveWritable, LongWritable, OsmEntityWritable]#Context): Unit = {
      tags = context.getConfiguration.get(FILTER_KEY).split(",")
      if (tags == null || tags.isEmpty) {
        throw new RuntimeException("No filter tag specified.")
      }
      filterByTags = EntityFilters.filterByTags(tags) _
    }

    def readBlob(osmBlock: ArrayPrimitiveWritable, osmDataType: String): Iterator[Entity] = {

      val bytes = osmBlock.get().asInstanceOf[Array[Byte]]
      val blobDecoder = new PbfBlobDecoder2(osmDataType, bytes)

      val l: util.ArrayList[EntityContainer] = new util.ArrayList[EntityContainer]()
      blobDecoder.runAndTrapExceptions(l)

      l.toIterator.map(_.getEntity) //.foreach(f => println(f.getEntity.getType))
    }

    override def map(key: Text, osmBlock: ArrayPrimitiveWritable, context: Mapper[Text, ArrayPrimitiveWritable, LongWritable, OsmEntityWritable]#Context): Unit = {

      val entities = readBlob(osmBlock, key.toString)

      for (value <- entities) {

        /*
        Way nodes are emitted alongside their node ids.
         */
        if (value.getType.equals(EntityType.Way) && filterByTags(value)) {
          val way = value.asInstanceOf[Way]

          way.getWayNodes.zipWithIndex.foreach(
            f => {
              k.set(f._1.getNodeId)
              wn.set((way.getId, f._2))
              v.set(wn)
              context.write(k, v)
            }
          )

          /*
          Ways are emitted alongside their way ids.
           */
          val wayWritable = new WayWritable() //purposefully recreated as it's dangerous to re-use a mapwritable

          for (t: String <- tags) {
            val theTag: Option[Tag] = value.getTags.find(_.getKey.equals(t))
            if (theTag.isDefined) {
              wayWritable.put(t, theTag.get.getValue)
            }
          }

          k.set(way.getId)
          v.set(wayWritable)

          context.write(k, v)
        }

        if (value.getType.equals(EntityType.Node)) {
          val node = value.asInstanceOf[Node]
          k.set(node.getId)
          nn.set((node.getLongitude, node.getLatitude))
          v.set(nn)
          context.write(k, v)
        }

      }
    }

  }

  /**
    * Waynodes and nodes are joined; ways are passed through unharmed.
    */
  class WayNodeReducer extends Reducer[LongWritable, OsmEntityWritable, LongWritable, OsmEntityWritable] {

    val outNode = new ReferencedWayNodeWritable()
    val k = new LongWritable()
    val v = new OsmEntityWritable()

    override def reduce(key: LongWritable, values: Iterable[OsmEntityWritable],
                        context: Reducer[LongWritable, OsmEntityWritable, LongWritable, OsmEntityWritable]#Context): Unit = {


      /*
      Partition the values into their respective types.
       */
      val z = values.map(_.get()).groupBy(_.getClass)
      val ww = z.get(classOf[WayWritable])
      val nn = z.get(classOf[NodeWritable])
      val wn = z.get(classOf[WayNodeWritable])

      /*
      Ways are simply written out with no further processing
       */
      if (ww.nonEmpty) {
        if (ww.size != 1) throw new RuntimeException("Found " + ww.size + " ways, there should only be one.")
        val way = ww.get.head.asInstanceOf[WayWritable]
        v.set(way)
        context.write(key, v)
      }

      /*
      WayNodes are written out as referenced way-nodes, i.e. way nodes with coordinates
       */
      if (wn.nonEmpty) {
        if (nn.size != 1) throw new RuntimeException("Found " + nn.size + " nodes, should be 1 for node: " + key.get())
        val nnw = nn.get.head.asInstanceOf[NodeWritable]
        for (wnw <- wn.get.map(_.asInstanceOf[WayNodeWritable])){
          k.set(wnw.wayId)
          outNode.set((wnw.ordinal, nnw.x, nnw.y))
          v.set(outNode)
          context.write(k, v)
        }
      }
    }
  }

}
