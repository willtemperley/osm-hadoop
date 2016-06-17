package org.roadlessforest.osm.writable

import java.util

import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.io.{MapWritable, Text, Writable}
import org.openstreetmap.osmosis.core.domain.v0_6.{Tag, Way}

import scala.collection.JavaConversions._

/**
  * Basically a way is just a collection of tags with an ID. This is just the value.
  *
  * Created by willtemperley@gmail.com on 21-Mar-16.
  */
class WayWritable() extends MapWritable {

  def set(entity: Way): Unit = {
    val tags: util.Collection[Tag] = entity.getTags

    tags.foreach(f => {
      put(f.getKey, f.getValue)
    })
  }

  def put(k: String, v: String): Unit = {
    put(new Text(k), new Text(v))
  }

}

object WayWritable {

  /*
  The key for a WKT geometry
   */
  val geometry = new Text("geometry")


}
