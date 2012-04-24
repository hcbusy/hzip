package org.yagnus.hzip.chunk.impl
import org.yagnus.hzip.CompressionMR
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.BytesWritable
import org.yagnus.hzip.chunk.SubChunk
import org.yagnus.hzip.chunk.ChunkIOs._;
import org.yagnus.yadoop.Yadoop._;
import org.slf4j.LoggerFactory

class ChunkMapper(processor: Array[Byte] => Array[Byte])
  extends Mapper[SubChunk, BytesWritable, SubChunkIO, BytesWritable]
  with CompressionMR[SubChunk, BytesWritable, SubChunkIO, BytesWritable] {

  private lazy val logger = LoggerFactory.getLogger(classOf[ChunkMapper]);

  def this() {
    this(x => x)
  }

  override protected def setup(context: MapperContext) {
    super.setup(context);
    super.configure(context.getConfiguration());
  }

  override protected def map(key: SubChunk, value: BytesWritable, context: MapperContext) {
    logger.info("Writing a sub chunk " + key + ", actual size is " + value.length);
    context.progress;
    context.write(key, processor(value));
  }

}
