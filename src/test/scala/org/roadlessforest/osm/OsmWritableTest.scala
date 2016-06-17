package org.roadlessforest.osm

import java.io.{ByteArrayInputStream, DataInputStream}

import org.apache.hadoop.io.{Writable, WritableUtils}
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode
import org.roadlessforest.osm.writable.{NodeWritable, ReferencedWayNodeWritable}
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.TableDrivenPropertyChecks

/**
  * Simple serialize-deserialize tests
  *
  * Created by willtemperley@gmail.com on 11-Mar-16.
  */
class OsmWritableTest extends WritableSpec {
  {
//    val x = (1 to 4).map(f => new OsmEntityWritable).toArray
//
//    x(0).set(new ReferencedWayNodeWritable)
//    x(1).set(new NodeWritable)
//    x(2).set(new WayWritable)
//    x(3).set(new ReferencedWayNodeWritable)
//
//    val z: Map[Class[_ <: Writable], Array[OsmEntityWritable]] = x.groupBy(_.get().getClass)
//
//    val ways = z.get(classOf[WayWritable])
//    val nodes = z.get(classOf[NodeWritable])
//    val wayNodeCoords = z.get(classOf[ReferencedWayNodeWritable])
//    val wayNodes = z.get(classOf[WayNodeWritable])
//
//    property("Groups of writables") {
//      ways.get should have length 1
//      nodes.get should have length 1
//      wayNodeCoords.get should have length 2
//      wayNodes shouldBe empty
//    }
  }

  {
    val nodeWritable = new NodeWritable
    nodeWritable.set((1.1, 2.2))
    val deserialized = new NodeWritable
    serializeDeserialize(nodeWritable, deserialized)

    property("Node writable") {
      deserialized.x should equal(1.1)
      deserialized.y should equal(2.2)
    }
  }

  {
    val wayNodeWritable = new ReferencedWayNodeWritable
    wayNodeWritable.set(20, 0.1, 0.2)
    val deserialized = new ReferencedWayNodeWritable
    serializeDeserialize(wayNodeWritable, deserialized)

    property("WayNode writable") {
      deserialized.ordinal should equal(20)
      deserialized.x should equal(0.1)
      deserialized.y should equal(0.2)
    }
  }

}

