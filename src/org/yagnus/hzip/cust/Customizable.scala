package org.yagnus.hzip.cust

/**
 * A customizable algorithm is one that can be instantiated with some
 * parameters after construction
 */
trait Customizable[C <: Customizations[C]] {

  protected var customization: C = _;
  def customize(in: C) { this.customization = in };
  def getCustomizations: C = customization;
}