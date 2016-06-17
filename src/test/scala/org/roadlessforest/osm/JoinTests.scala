package org.roadlessforest.osm

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}
/**
  * Just to test my knowledge of the API!
  *
  * Created by willtemperley@gmail.com on 26-Feb-16.
  */
class JoinTests extends PropSpec with TableDrivenPropertyChecks with Matchers  {

  val conf = new SparkConf().setMaster("local[2]").setAppName("basic-tests")
  val sc = new SparkContext(conf)

  val x = sc.parallelize(Seq((1l, "a"), (2l, "b"), (3l, "c")))
  val y = sc.parallelize(Seq((1l, "A"), (2l, "B"), (3l, "C")))

  val xy = x.join(y)

  xy.foreach(println)

}
