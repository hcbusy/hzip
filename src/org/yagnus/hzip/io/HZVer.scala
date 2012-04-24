package org.yagnus.hzip.io
import org.yagnus.yadoop.io._;
import org.yagnus.hzip.AppConstants
import scala.reflect.BeanProperty

trait HZVer extends Versioned {
  @BeanProperty
  var version: String = null;
  def addVersion(v: Any) {
    if (version == null) version = AppConstants.LATEST_HZIP_APP_VERSION; ;
    setVersion(version + ";" + v);
  }
  def addVersion(i: Int) {
    addVersion("" + i);
  }

  /*
   * Implementing class should override. Default implementation
   * uses string comparison to see if the other object greater than current object's version.
   */
  def canUseVersion(otherVersion: String): Boolean = otherVersion > version;
}