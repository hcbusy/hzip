package org.yagnus.hzip.chunk.impl
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.Partitioner
import org.yagnus.hzip.chunk.ChunkIOs._;
import org.yagnus.hzip.chunk.Chunk

class ChunkPartitioner extends Partitioner[Chunk, Object] {
    def getPartition(key: Chunk, value: Object, numPartitions: Int): Int = (key.getChunkId % numPartitions).intValue;
}