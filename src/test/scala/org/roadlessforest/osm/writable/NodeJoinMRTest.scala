package org.roadlessforest.osm.writable

import java.io.{DataInputStream, File, FileInputStream}

import org.apache.hadoop.io.{ArrayPrimitiveWritable, LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapReduceDriver, ReduceDriver}
import org.junit.Test
import org.openstreetmap.osmosis.pbf2.v0_6.impl.{PbfRawBlob, PbfStreamSplitter}
import org.roadlessforest.osm.NodeJoiner.{OsmEntityMapper, WayNodeReducer}

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 07-Apr-16.
  */
class NodeJoinMRTest {

  //  super.setupSerialization(new Map)
  val snapshotBinaryFile: File = new File("src/test/resources/data/template/v0_6/db-snapshot.pbf")


  val mapReduceDriver: MapReduceDriver[Text, ArrayPrimitiveWritable, LongWritable, OsmEntityWritable, LongWritable, OsmEntityWritable]
  = MapReduceDriver.newMapReduceDriver()

  mapReduceDriver.setMapper(new OsmEntityMapper)
  mapReduceDriver.setReducer(new WayNodeReducer)


  @Test
  def testMR() {

    val inputStream = new FileInputStream(snapshotBinaryFile)
    val streamSplitter = new PbfStreamSplitter(new DataInputStream(inputStream))

    val arrayPrimitiveWritable: ArrayPrimitiveWritable = new ArrayPrimitiveWritable
    val text: Text = new Text

    while (streamSplitter.hasNext) {
      val blob: PbfRawBlob = streamSplitter.next
      arrayPrimitiveWritable.set(blob.getData)
      val `type`: String = blob.getType
      text.set(`type`)
      mapReduceDriver.withInput(text, arrayPrimitiveWritable)
    }

    //Retrieve MR results
    val results = mapReduceDriver.run
    for (cellPair <- results) {
      println(cellPair)
    }

    //    reduceDriver.withInput(new LongWritable(1), osmEntityWritable.toList)


    //    reduceDriver.withOutput(new LongWritable(1), )
    //    mapRe.withInput(new LongWritable(), wayWritable)

    //    reduceDriver.runTest()


  }


  var reducer = new WayNodeReducer
  var mapper = new OsmEntityMapper

  var reduceDriver = ReduceDriver.newReduceDriver(reducer)

  val osmEntityWritable: Array[OsmEntityWritable] = (1 to 4).map(f => new OsmEntityWritable).toArray

  private val referencedWayNodeWritable = new ReferencedWayNodeWritable
  private val referencedWayNodeWritable2 = new ReferencedWayNodeWritable
  private val nodeWritable = new NodeWritable
  private val writable = new WayWritable

  writable.put(new Text("a"), new Text("b"))
  referencedWayNodeWritable.set((1, 2, 2))
  referencedWayNodeWritable2.set((2, 3, 4))
  nodeWritable.set((.1d, 2d))

  osmEntityWritable(0).set(referencedWayNodeWritable)
  osmEntityWritable(1).set(nodeWritable)
  osmEntityWritable(2).set(writable)
  osmEntityWritable(3).set(referencedWayNodeWritable2)

  @Test
  def testReducer() {


    //    reduceDriver.withInput(new LongWritable(1), osmEntityWritable.toList)


    //    reduceDriver.withOutput(new LongWritable(1), )
    //    mapRe.withInput(new LongWritable(), wayWritable)

    //    reduceDriver.runTest()


  }

}
