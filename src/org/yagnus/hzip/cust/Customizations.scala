package org.yagnus.hzip.cust
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.reflect.BeanProperty
import org.yagnus.hzip.io.HZIO
/**
 * Customizations contain numeric and string key value pairs
 */
trait Customizations[SELF <: Customizations[SELF]] extends HZIO[SELF] {
  this: SELF =>
  setVersion("0");

  @BeanProperty
  var strs: Map[String, String] = new HashMap[String, String];

  @BeanProperty
  var floats: Map[String, Double] = new HashMap[String, Double];

  @BeanProperty
  var ints: Map[String, Long] = new HashMap[String, Long];

  def update(key: String, value: String) { strs(key) = value }
  def update(key: String, value: Float) { floats(key) = value }
  def update(key: String, value: Double) { floats(key) = value }
  def update(key: String, value: Byte) { ints(key) = value }
  def update(key: String, value: Short) { ints(key) = value }
  def update(key: String, value: Int) { ints(key) = value }
  def update(key: String, value: Long) { ints(key) = value }

  def apply(key: String): String = strs(key);
  def getStr(key: String): String = apply(key);
  def getFloat(key: String): Double = floats.get(key).getOrElse(Double.NaN);
  def getFixed(key: String): Long = ints.get(key).getOrElse(-1);
}