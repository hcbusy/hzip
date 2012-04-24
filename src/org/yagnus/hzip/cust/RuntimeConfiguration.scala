package org.yagnus.hzip.cust
import scala.reflect.BeanProperty
import org.yagnus.hzip.io.HZIO

/**
 * This class holds configuration used while running such as
 * where the temp dir is etc.s
 */
class RuntimeConfiguration extends HZIO[RuntimeConfiguration] {
    protected def getSerializationClass = classOf[RuntimeConfiguration];

    addVersion(1);
    @BeanProperty
    var tempDir = "/tmp";

    @BeanProperty
    var localTempDir = "/tmp";
}