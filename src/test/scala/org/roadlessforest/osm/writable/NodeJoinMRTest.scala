package org.roadlessforest.osm.writable

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, MapReduceDriver, ReduceDriver}
import org.junit.Test
import org.roadlessforest.osm.NodeJoiner.{OsmEntityMapper, WayNodeReducer}

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 07-Apr-16.
  */
class NodeJoinMRTest {

  var reducer = new WayNodeReducer
  var mapper = new OsmEntityMapper


  var reduceDriver = ReduceDriver.newReduceDriver(reducer)

  val x = (1 to 4).map(f => new OsmEntityWritable).toArray

  private val wayNodeCoordinatesWritable = new ReferencedWayNodeWritable
  private val wayNodeCoordinatesWritable1 = new ReferencedWayNodeWritable
  private val nodeWritable = new NodeWritable
  private val writable = new WayWritable

  writable.put(new Text("a"), new Text("b"))
  wayNodeCoordinatesWritable.set((1,2,2))
  wayNodeCoordinatesWritable1.set((2,3,4))
  nodeWritable.set((.1d, 2d))

  x(0).set(wayNodeCoordinatesWritable)
  x(1).set(nodeWritable)
  x(2).set(writable)
  x(3).set(wayNodeCoordinatesWritable1)

  @Test
  def testReducer() {


    reduceDriver.withInput(new LongWritable(1), x.toList)

//    mapRe.withInput(new LongWritable(), wayWritable)

    reduceDriver.runTest()


  }

}
