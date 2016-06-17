package org.roadlessforest.osm

import java.util.Date

import org.openstreetmap.osmosis.core.domain.v0_6.{Tag, _}
import org.roadlessforest.osm.filter.EntityFilters
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.JavaConversions._


/**
  * Tests for tag-based filters
  *
  * Created by willtemperley@gmail.com on 23-Feb-16.
  */
class FilterTests extends PropSpec with TableDrivenPropertyChecks with Matchers {

  //Create some dummy tags for the given keys
  def fakeSomeTags(keys: String*): Seq[Tag] = {
    keys.map(f => new Tag(f, "x"))
  }

  val fakeWayNodes = Seq()

  val hasBoth = new Way(new CommonEntityData(1l, 1, new Date(), null, 1, fakeSomeTags("highway", "railway")), fakeWayNodes)
  val hasHighway = new Way(new CommonEntityData(1l, 1, new Date(), null, 1, fakeSomeTags("highway", "nothing")), fakeWayNodes)
  val hasHighway2 = new Way(new CommonEntityData(1l, 1, new Date(), null, 1, fakeSomeTags("waterway", "highway")), fakeWayNodes)
  val hasRailway = new Way(new CommonEntityData(1l, 1, new Date(), null, 1, fakeSomeTags("railway")), fakeWayNodes)
  val hasDmmyTags = new Way(new CommonEntityData(1l, 1, new Date(), null, 1, fakeSomeTags("do", "not", "match")), fakeWayNodes)
  val zeroTags = new Way(new CommonEntityData(1l, 1, new Date(), null, 1, fakeSomeTags()), fakeWayNodes)

  val ways = Seq(hasBoth, hasDmmyTags, hasHighway, hasHighway2, hasRailway, zeroTags)

  val highwayFilter = EntityFilters.filterByTags("highway") _
  val railwayFilter = EntityFilters.filterByTags("railway") _
  val transportFilter = EntityFilters.filterByTags("railway", "highway") _
  val filterByTags = EntityFilters.filterByTags("shouldnotmatch") _

  property("Way types") {
    ways.filter(filterByTags) should have length 0
    ways.filter(highwayFilter) should have length 3
    ways.filter(railwayFilter) should have length 2
    ways.filter(transportFilter) should have length 4
  }

}