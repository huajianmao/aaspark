package cn.hjmao.aaspark.ch08

import spray.json.JsValue

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

class GeoJson {
}
