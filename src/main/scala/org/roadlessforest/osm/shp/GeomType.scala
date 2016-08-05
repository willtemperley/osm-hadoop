package org.roadlessforest.osm.shp

/**
  * Created by willtemperley@gmail.com on 22-Jan-16.
  *
  * Why the chuff this doesn't exist somewhere is strange
  *
  *
  */
//TODO move to core package
object GeomType extends Enumeration {

    val
    Geometry,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    CircularString,
    CompoundCurve,
    CurvePolygon,
    MultiCurve,
    MultiSurface,
    Curve,
    Surface,
    PolyhedralSurface,
    TIN,
    Triangle = Value
}
