package org.roadlessforest.osm.config

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/**
 * Just a place to keep the config in one place
 *
 * Created by tempehu on 22-Dec-14.
 */
object ConfigurationFactory {

  def get: Configuration = {

    val conf = new Configuration
    hbaseProperties.foreach(f => conf.set(f._1, f._2))
    conf
  }

//  def main(args: Array[String]): Unit = {
//    getSparkConf
//  }

  def getSparkConf: SparkConf = {

    val conf = new SparkConf
    hbaseProperties.foreach(f => conf.set(f._1, f._2))
    conf
  }


  def hbaseProperties: Properties = {
    val props: Properties = getProperties("hbase-config.properties")
    props
  }

  def getPrecedence: Map[Int, Int] = {

    val props: Properties = getProperties("raster-priority.properties")
    props.toMap.map(f => f._1.toInt -> f._2.toInt)
  }

  def getProperties(propertiesFileName: String): Properties = {
    val props = new Properties()
    val loader = Thread.currentThread().getContextClassLoader
    val resourceStream = loader.getResourceAsStream(propertiesFileName)
    props.load(resourceStream)
    props
  }
}
