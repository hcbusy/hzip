package org.yagnus.hzip

import scala.xml._;
import org.yagnus.hzip.algorithms.Algorithms;
import org.yagnus.hzip.HdfsUtils.{ XmlIOer, XmlCompatible };
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class MetaData extends XmlCompatible[MetaData] {
    private lazy val _l = new HZLogger(classOf[MetaData]);
    var algorithm : Algorithms.Algorithms = Algorithms.unknown;
    var version : String = Common.currentVersion;

    def toXml() =
        <HzipMetaData>
            <Version>
                { version }
            </Version>
            <Algorithm>
                { algorithm.toString }
            </Algorithm>
        </HzipMetaData>

    def fromXml(xml : scala.xml.Elem) : MetaData = {
        xml match {
            case origSpec@ <HzipMetaData>{ metaD@_* }</HzipMetaData> ⇒
                val algoname = (metaD \\ "Algorithm").text.trim;
                println("Algorithm is"+algoname);
                this.algorithm = Algorithms.withName(algoname);
                this.version = (metaD \\ "Version").text.toString.trim;
        }
        return this;
    }
}

object MetaData extends XmlIOer[MetaData] {

    def apply : MetaData = new MetaData();

    def constructObject = new MetaData();

}
