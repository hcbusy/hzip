package org.yagnus.hzip.chunk.impl;
import org.yagnus.hzip.chunk._;

/**
  * TODO: change serialization so that this can be rawcompared
  */
class SubChunkGrouper extends SubChunkRawComparator {

    def compare(a: SubChunk, b: SubChunk): Int = {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a.getChunkId == b.getChunkId) return 0;
        if (a.getChunkId > b.getChunkId) return 1;
        return -1;
    }
}