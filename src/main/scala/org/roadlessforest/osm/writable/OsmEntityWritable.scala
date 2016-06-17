package org.roadlessforest.osm.writable

import org.apache.hadoop.io.{Writable, GenericWritable}

/**
  * GenericWritables allow a polymorphic type to be passed to the reducer, with minimised overhead.
  * This simply allows all entities to be written as one type and the framework knows how to deserialize based on it's class.
  *
  * Created by willtemperley@gmail.com on 11-Mar-16.
  */
class OsmEntityWritable extends GenericWritable {

  val classes: Array[Class[_ <: Writable]] =
    Array(
      classOf[ReferencedWayNodeWritable],
      classOf[WayWritable],
      classOf[WayNodeWritable],
      classOf[NodeWritable]
    )

  override def getTypes: Array[Class[_ <: Writable]] = classes

}
