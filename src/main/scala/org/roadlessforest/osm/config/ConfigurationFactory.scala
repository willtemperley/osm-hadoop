package org.roadlessforest.osm.config

import java.util.Properties

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._

/**
 * Just a place to keep the config in one place
 *
 * Created by tempehu on 22-Dec-14.
 */
object ConfigurationFactory {


  def get: Configuration = {


    val props = new Properties()

    val loader = Thread.currentThread().getContextClassLoader

    val resourceStream = loader.getResourceAsStream("hbase-config.properties")
    props.load(resourceStream)

    val conf = new Configuration

    props.keys().foreach(f => conf.set(f.toString, props.get(f.toString).toString))

    conf
  }

}
