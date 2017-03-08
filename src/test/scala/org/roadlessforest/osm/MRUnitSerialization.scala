package org.roadlessforest.osm
import java.nio.file.{Path, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.{KeyValueSerialization, MutationSerialization, ResultSerialization}
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver

/**
  * Created by willtemperley@gmail.com on 23-Feb-17.
  */
trait MRUnitSerialization {

  protected def setupSerialization(mapReduceDriver: MapReduceDriver[_, _, _, _, _, _]): Configuration = {
    val configuration = mapReduceDriver.getConfiguration
    configuration.setStrings("io.serializations", configuration.get("io.serializations"), classOf[MutationSerialization].getName, classOf[ResultSerialization].getName, classOf[KeyValueSerialization].getName)
    configuration
  }

  def getResource(resourceName: String): String = {
    ClassLoader.getSystemResource(resourceName).toString
  }

}
