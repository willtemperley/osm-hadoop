package org.roadlessforest.osm.buffer

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan, Table}
import xyz.TileCalculator.Tile

/**
  * Created by willtemperley@gmail.com on 22-Nov-16.
  */
object HBaseTest {

  def main(args: Array[String]): Unit = {

    val tab = getTable("buffer14")

    val scan = new Scan()
    scan.addFamily(Tile.cf)
    val res = tab.getScanner(scan)

    val iterator = res.iterator()
    while (iterator.hasNext) {

      val res = iterator.next()
      val v = res.getValue(Tile.cf, Tile.cimg)

      val t = new Tile(res.getRow)
      println(t)


    }

  }

  /**
    * Loads and wraps an hbase connection
    *
    * Created by willtemperley@gmail.com on 14-Jul-16.
    */

  def getTable(tableName: String): Table = connection.getTable(TableName.valueOf(tableName))

  val connection = ConnectionFactory.createConnection(getConfiguration)

  def getConfiguration: Configuration = {

    val configuration = new Configuration()
    val props = new Properties()
    val loader = Thread.currentThread().getContextClassLoader
    val resourceStream = loader.getResourceAsStream("hbase-config.properties")

    props.load(resourceStream)

    val quorum = "hbase.zookeeper.quorum"
    val master = "hbase.master"
    configuration.set(quorum, props.getProperty(quorum))
    configuration.set(master, props.getProperty(master))

    configuration
  }
}
