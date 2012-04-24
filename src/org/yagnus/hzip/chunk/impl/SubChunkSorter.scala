package org.yagnus.hzip.chunk.impl;
import org.apache.hadoop.io.RawComparator
import org.yagnus.hzip.chunk.ChunkIOs._;
import org.yagnus.hzip.chunk._;

/*
 * TODO: change serialization so that this can be rawcompared
 */
class SubChunkSorter extends SubChunkRawComparator {

  def compare(a: SubChunk, b: SubChunk): Int = {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    if (a.getChunkId == b.getChunkId) {
      if (a.getOffset == b.getOffset) return 0;
      if (a.getOffset > b.getOffset) return 1;
      return -1;
    }
    if (a.getChunkId > b.getChunkId) return 1;
    return -1;
  }
}