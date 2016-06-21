package org.roadlessforest.osm
import java.util

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer
import org.openstreetmap.osmosis.core.domain.v0_6.Entity
import org.openstreetmap.osmosis.pbf2.v0_6.impl.{PbfBlobDecoder, PbfBlobDecoderListener}

import scala.collection.JavaConversions._

/**
  * Mixed in with classes that need to decode OSM pbf format data
  *
  * Created by willtemperley@gmail.com on 21-Jun-16.
  */
trait DecodesOsm {

  def readBlob(bytes: Array[Byte], osmDataType: String): Iterator[Entity] = {

    val decoderListener = new PbfBlobDecoderListener {

      var entityData: util.List[EntityContainer] = _

      override def error(): Unit = {}

      override def complete(decodedEntities: util.List[EntityContainer]): Unit = {
        entityData = decodedEntities
      }
    }

    val blobDecoder = new PbfBlobDecoder(osmDataType, bytes, decoderListener)
    blobDecoder.run()
    decoderListener.entityData.toIterator.map(_.getEntity)
  }
}
