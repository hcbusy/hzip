package org.yagnus.hzip.chunk.impl;
import org.yagnus.yadoop.LongArrayWritable
import org.apache.hadoop.mapreduce.Partitioner
import org.yagnus.hzip.chunk.ChunkIOs._
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.LongWritable
import org.yagnus.hzip.chunk.SubChunk
import org.yagnus.hzip.HZLogger
import org.yagnus.hzip.io.HZVer

//class SubChunkPartitioner extends Partitioner[SubChunkIO, BytesWritable] with HZLogger with HZVer {
//    private lazy implicit final val LOG_AS = initLogger(classOf[SubChunkPartitioner]);
//    addVersion("0.0.1");
//    // The batch number is the only thing we partition by.
//    def getPartition(key: SubChunkIO, value: BytesWritable, numPartitions: Int): Int = {
//        val ret = (key.getChunkId() % numPartitions).intValue;
//        info("Partitioner received this subChunk:" + key + " will partition into:" + ret)
//        return ret;
//    }
//}
class SubChunkPartitioner extends Partitioner[SubChunk, BytesWritable] {
    // The batch number is the only thing we partition by.
    def getPartition(key: SubChunk, value: BytesWritable, numPartitions: Int): Int =
        (key.getChunkId % numPartitions).intValue;
}
