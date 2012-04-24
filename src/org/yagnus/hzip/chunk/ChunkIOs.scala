package org.yagnus.hzip.chunk
import org.apache.hadoop.io.Writable
import org.yagnus.hzip.io.HZIO
import scala.reflect.BeanProperty

object ChunkIOs {
  /**
   * This is an intermediate object that contains a subChunk meta data and it's actual bytes
   */
  class SubChunkDataIO extends SubChunk
    with Writable with HZIO[SubChunk]
    with Ordered[SubChunk] {
    protected def getSerializationClass = classOf[SubChunk];

    @BeanProperty
    var data: Array[Byte] = _;
  }
  class SubChunkIO extends SubChunk
    with Writable with HZIO[SubChunk]
    with Comparable[SubChunk] {
    protected def getSerializationClass = classOf[SubChunk];
  }
  class ChunkDataIO extends Chunk
    with Ordered[Chunk] with HZIO[Chunk] {

    protected def getSerializationClass = classOf[Chunk];

    @BeanProperty
    var data: Array[Byte] = _;

    def set(chunkId: Long, size: Long) {
      setChunkId(chunkId);
      setLength(size);
    }

    def set(o: Chunk) {
      set(o.getChunkId, o.getLength);
    }

    def compare(o: ChunkDataIO): Int = {
      if (getChunkId > o.getChunkId) return 1;
      if (getChunkId == o.getChunkId) return 0;
      return -1;
    }
  }

  class ChunkIO extends Chunk
    with Ordered[Chunk] with HZIO[Chunk] {

    protected def getSerializationClass = classOf[Chunk];

  }

  class ChunksIO extends Chunks with HZIO[Chunks] {
    addVersion(1);
    protected def getSerializationClass = classOf[Chunks];

  }

  implicit def fromChunktoChunkIO(c: Chunk): ChunkIO = {
    val cio = new ChunkIO;
    cio.setChunkId(c.getChunkId);
    cio.setLength(c.getLength);
    cio.setSubChunks(c.getSubChunks);
    cio.setVersion(c.getVersion());
    return cio;
  }

  implicit def fromSubChunktoSubChunkIO(sc: SubChunk): SubChunkIO = {
    val scio = new SubChunkIO;
    scio.set(sc);
    return scio;
  }
}