package org.yagnus.hzip.chunk;

import scala.collection.mutable.HashMap
import scala.reflect.BeanProperty
import org.yagnus.hzip.io.HZIO
import org.yagnus.hzip.CompressionAlgorithmFactory
import org.apache.hadoop.fs.FileSystem
import org.yagnus.hzip.chunk.ChunkIOs._;

/**
 *
 * A Chunker is something that makes chunk of bigger things
 *
 */
class Chunker extends CompressionAlgorithmFactory {
    @BeanProperty var myChunks: ChunksIO = new ChunksIO();
}

object ChunkerSerailization extends App {
    import scala.collection.mutable.{ Map, HashMap }

    class Shit extends HZIO[Shit] {
        override def getSerializationClass = classOf[Shit];
        addVersion("erg.");
        @BeanProperty var m: HashMap[String, String] = new HashMap[String, String]();
        @BeanProperty var ml: HashMap[java.lang.Long, String] = new HashMap[java.lang.Long, String]();
        //    @BeanProperty var arr2: java.util.ArrayList[java.lang.Integer] = new java.util.ArrayList[java.lang.Integer]();
        //    var m2: java.util.Map[java.lang.Integer, java.lang.Integer] = new java.util.HashMap[java.lang.Integer, java.lang.Integer]();
        @BeanProperty var arr: Array[Int] = Array(1, 2, 3, 4);
        @BeanProperty var a: Int = 10;
        @BeanProperty var m3: HashMap[String, List[java.lang.Long]] = new HashMap[String, List[java.lang.Long]]();
    }

}