package org.roadlessforest.osm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ColumnName, DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.roadlessforest.osm.config.ConfigurationFactory
import org.apache.spark.sql.functions._

/**
  * Created by willtemperley@gmail.com on 24-Aug-16.
  */
object Relations {

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  def main(args: Array[String]): Unit = {

    def catalog =
      s"""{
        "table":{"namespace":"default", "name":"relations"},
        "rowkey":"key",
        "columns":{
          "col0":{"cf":"rowkey", "col":"key", "type":"long"},
          "col1":{"cf":"t", "col":"name", "type":"string"}
        }
      }""".stripMargin


    val conf = new SparkConf()
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
