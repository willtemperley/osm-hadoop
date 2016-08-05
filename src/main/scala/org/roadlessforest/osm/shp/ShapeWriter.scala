package org.roadlessforest.osm.shp

import java.io.File
import java.util

import com.vividsolutions.jts.geom._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataUtilities, DefaultTransaction, Transaction}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


/**
 * Store features then write them to a shapefile
 */
class ShapeWriter(val geomType: GeomType.Value = GeomType.Point, val srid: Int = 4326, schemaDef: String = "highway:String") {

  val schema = "the_geom:" + geomType + ":srid=" + srid + "," + schemaDef

//  val TYPE= DataUtilities.createType("Location", "the_geom:Polygon:srid=4326," + "i:Integer," + "j:Integer")
  val SIMPLE_FEATURE_TYPE: SimpleFeatureType = DataUtilities.createType("Location", schema)

  val features = new util.ArrayList[SimpleFeature]

//  private def getCoords(env: Envelope): Array[Coordinate] = {
//    val c1: Coordinate = new Coordinate(env.getMinX, env.getMinY)
//    val c2: Coordinate = new Coordinate(env.getMinX, env.getMaxY)
//    val c3: Coordinate = new Coordinate(env.getMaxX, env.getMaxY)
//    val c4: Coordinate = new Coordinate(env.getMaxX, env.getMinY)
//    val c5: Coordinate = new Coordinate(env.getMinX, env.getMinY)
//    val c: Array[Coordinate] = Array[Coordinate](c1, c2, c3, c4, c5)
//    return c
//  }

  def addFeature(geometry: Geometry, attrs: Seq[Any] = null): SimpleFeature = {

    if (geometry == null) return null

    val featureBuilder: SimpleFeatureBuilder = new SimpleFeatureBuilder(SIMPLE_FEATURE_TYPE)
    featureBuilder.add(geometry)
    if (attrs != null) {
      attrs.foreach(featureBuilder.add)
    }
    val feature: SimpleFeature = featureBuilder.buildFeature(null)
    features.add(feature)

    if (features.size() % 10000 == 0) {
      println("Processed: " + features.size())
    }
    feature
  }

  @throws(classOf[Exception])
  def write(fileLocation: String = "temp.shp") {

    val newFile: File = new File(fileLocation)
    val dataStoreFactory: ShapefileDataStoreFactory = new ShapefileDataStoreFactory
    val params: util.Map[String, java.io.Serializable] = new util.HashMap[String, java.io.Serializable]

    print("Writing " + features.size() + " features")
    println(" to " + newFile.getAbsolutePath)

    params.put("create spatial index", "true")
    params.put("url", "file://" + newFile.getAbsolutePath)

    val newDataStore = dataStoreFactory.createNewDataStore(params).asInstanceOf[ShapefileDataStore]
    System.out.println("TYPE:" + SIMPLE_FEATURE_TYPE)
    newDataStore.createSchema(SIMPLE_FEATURE_TYPE)

    val transaction: Transaction = new DefaultTransaction("create")
    val typeName: String = newDataStore.getTypeNames()(0)
    val featureSource = newDataStore.getFeatureSource(typeName)
//    val SHAPE_TYPE: SimpleFeatureType = featureSource.getSchema
//    System.out.println("SHAPE:" + SHAPE_TYPE)

    featureSource match {
      case featureStore: SimpleFeatureStore =>
        val collection = new ListFeatureCollection(SIMPLE_FEATURE_TYPE, features)
        featureStore.setTransaction(transaction)
        try {
          featureStore.addFeatures(collection)
          transaction.commit()
        }
        catch {
          case problem: Exception => {
            problem.printStackTrace()
            transaction.rollback()
          }
        } finally {
          transaction.close()
        }
      case _ =>
        System.out.println(typeName + " does not support read/write access")
    }
  }

}
