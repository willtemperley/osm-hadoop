package org.roadlessforest.osm


import com.esri.core.geometry.{Operator, OperatorFactoryLocal, OperatorGeodesicBuffer, Polyline}
import org.junit.Test

/**
  * Created by willtemperley@gmail.com on 15-Nov-16.
  */
object GeodesicDistanceTest {

  @Test
  def go(): Unit = {

    val p = new Polyline()
    p.startPath(0,0)
    p.lineTo(0, 1)

    OperatorFactoryLocal.getInstance().getOperator(Operator.Type.GeodeticLength)
  }

}
