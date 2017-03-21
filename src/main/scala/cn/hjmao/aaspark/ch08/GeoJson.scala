package cn.hjmao.aaspark.ch08

import com.esri.core.geometry.{Geometry, GeometryEngine}
import spray.json._

/**
 * Created by hjmao on 17-3-20.
 */

case class Feature(val id: Option[JsValue],
                   val properties: Map[String, JsValue],
                   val geometry: RichGeometry) {
  def apply(property: String): JsValue = properties(property)
  def get(property: String): Option[JsValue] = properties.get(property)
}

case class FeatureCollection(features: Array[Feature]) extends IndexedSeq[Feature] {
  def apply(index: Int): Feature = features(index)
  def length: Int = features.length
}

case class GeometryCollection(geometries: Array[RichGeometry]) extends IndexedSeq[RichGeometry] {
  def apply(index: Int): RichGeometry = geometries(index)
  def length: Int = geometries.length
}

object GeoJsonProtocol extends DefaultJsonProtocol {

  implicit object RichGeometryJsonFormat extends RootJsonFormat[RichGeometry] {
    def write(rg: RichGeometry): JsValue = {
      GeometryEngine.geometryToGeoJson(rg.spatialReference, rg.geometry).parseJson
    }

    def read(value: JsValue): RichGeometry = {
      val mg = GeometryEngine.geometryFromGeoJson(value.compactPrint, 0, Geometry.Type.Unknown)
      new RichGeometry(mg.getGeometry, mg.getSpatialReference)
    }
  }

  implicit object FeatureJsonFormat extends RootJsonFormat[Feature] {
    def write(f: Feature): JsObject = {
      val buf = scala.collection.mutable.ArrayBuffer(
        "type" -> JsString("Feature"),
        "properties" -> JsObject(f.properties),
        "geometry" -> f.geometry.toJson
      )
      f.id.foreach(v => {
        buf += "id" -> v
      })
      JsObject(buf.toMap)
    }

    def read(value: JsValue): Feature = {
      val jso = value.asJsObject
      val id = jso.fields.get("id")
      val properties = jso.fields("properties").asJsObject.fields
      val geometry = jso.fields("geometry").convertTo[RichGeometry]
      Feature(id, properties, geometry)
    }
  }

  implicit object FeatureCollectionJsonFormat extends RootJsonFormat[FeatureCollection] {
    def write(fc: FeatureCollection): JsObject = {
      JsObject(
        "type" -> JsString("FeatureCollection"),
        "features" -> JsArray(fc.features.map(_.toJson): _*)
      )
    }
    def read(value: JsValue): FeatureCollection = {
      FeatureCollection(value.asJsObject.fields("features").convertTo[Array[Feature]])
    }
  }
  implicit object GeometryCollectionJsonFomat extends RootJsonFormat[GeometryCollection] {
    def write(gc: GeometryCollection): JsObject = {
      JsObject(
        "type" -> JsString("GeometryCollection"),
        "geometries" -> JsArray(gc.geometries.map(_.toJson): _*)
      )
    }
    def read(value: JsValue): GeometryCollection = {
      GeometryCollection(value.asJsObject.fields("geometries").convertTo[Array[RichGeometry]])
    }
  }
}
