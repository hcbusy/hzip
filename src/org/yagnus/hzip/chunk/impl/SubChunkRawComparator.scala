package org.yagnus.hzip.chunk.impl;
import org.apache.hadoop.io.RawComparator
import org.yagnus.hzip.chunk.ChunkIOs._;
import org.slf4j.LoggerFactory
import org.yagnus.hzip.chunk._;

trait SubChunkRawComparator extends RawComparator[SubChunk] {

  def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
    //because native serialization writes a integer length before the json/xml/protobuff starts
    val a = new SubChunkIO().deserializeFromBytes(b1, s1 + 4, l1 - 4);
    val b = new SubChunkIO().deserializeFromBytes(b2, s2 + 4, l2 - 4);
    return compare(a, b);
  }

  def compare(a: SubChunk, b: SubChunk): Int;

}
