package org.roadlessforest.osm

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, Filter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ColumnName, DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.roadlessforest.osm.config.ConfigurationFactory
import org.apache.spark.sql.functions._

/**
  * Created by willtemperley@gmail.com on 24-Aug-16.
  */
object Relations {

  private val connection = ConnectionFactory.createConnection(ConfigurationFactory.get)

//  private def getConfiguration: Configuration = {
//    val configuration: Configuration = new Configuration
//    configuration.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-m1,hadoop-01")
//    configuration.set("hbase.master", "hadoop-m2")
//    configuration
//  }

  def getTable(tableName: String): Table = connection.getTable(TableName.valueOf(tableName))

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  val cf = "t".getBytes

  def main(args: Array[String]): Unit = {

    val table = getTable("relations")
    println(table.getName)
    val scan = new Scan
    scan.addFamily(cf)
    val filter = new SingleColumnValueFilter(cf, "boundary".getBytes, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("national_park")))
    scan.setFilter(filter)

    table.getScanner(scan)
    // Getting the scan result
    val scanner = table.getScanner(scan);

    val iterator = Iterator.continually(scanner.next).takeWhile(_ != null)

    for (x <- iterator) {
      val value = x.getValue(cf, "name".getBytes)
      if (value != null) {
        println(Bytes.toString(value))
      }
    }

  }

  def x() {

    def catalog =
      s"""{
        "table":{"namespace":"default", "name":"relations"},
        "rowkey":"key",
        "columns":{
          "col0":{"cf":"rowkey", "col":"key", "type":"long"},
          "col1":{"cf":"t", "col":"name", "type":"string"}
        }
      }""".stripMargin


    val conf = ConfigurationFactory.getSparkConf
    conf.setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val dataFrame: DataFrame = withCatalog(catalog)
    val r = dataFrame.filter($"col0" like "Addo%")

    r.show()

//    dataFrame.sh
//    dataFrame.

  }

}
