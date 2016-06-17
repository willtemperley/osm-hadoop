package org.roadlessforest.osm.filter

import org.openstreetmap.osmosis.core.domain.v0_6.Entity

import scala.collection.JavaConversions._

/**
  * Created by willtemperley@gmail.com on 25-Feb-16.
  */
object EntityFilters extends Serializable {

  /**
  * Given an entity, does it have at least one of the key(s)
  *
  * @param keys the keys to test for the existence of
  * @param entity the entity in question
  * @return if one of the keys exists
  */
  def filterByTags(keys: String *)(entity: Entity): Boolean = {
    entity.getTags.exists(f => keys.contains(f.getKey))
  }

  def filterByTags(keys: Array[String])(entity: Entity): Boolean = filterByTags(keys:_*)(entity)
}
