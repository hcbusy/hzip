package org.yagnus.hzip.fileIo

import java.nio.ByteBuffer
import org.yagnus.containers.Accessibles._;

object NioAccessible {
  import java.nio.ByteBuffer;
  final class FileMappedIndexedAccessible(private val src: ByteBuffer)
    extends IndexedAccessible[Byte]
    with IndexedDataAccessible {

    def apply(idx: Int): Byte = get(idx);
    def iterator = new Iterator[Byte] {
      var idx = 0;
      def hasNext = idx < src.limit;
      def next = { val ret = src.get(idx); idx += 1; ret }
    }
    def toArray = null;
    override def size = src.limit.intValue;

    final def get(index: Long): Byte = src.get(index.intValue);

    final def get(dst: Array[Byte], offset: Long, length: Long) {
      src.get(dst, offset.intValue, length.intValue);
    }

    final override def toString: String = "BufferByteGettable(len=" + src.limit + ")";

    //The data gettable implemetnations
    final def getByte(ind: Long): Byte = get(ind);
    final def getInt(ind: Long): Int = src.getInt(ind.intValue);
    final def getShort(ind: Long): Short = src.getShort(ind.intValue);
    final def getLong(ind: Long) = src.getLong(ind.intValue);
    final def getChar(ind: Long): Char = src.getChar(ind.intValue);
    final def getDouble(ind: Long): Double = src.getDouble(ind.intValue);
    final def getFloat(ind: Long): Float = src.getFloat(ind.intValue);

  }

  implicit def convertByteBufferToAccessible(x: ByteBuffer) = new FileMappedIndexedAccessible(x);
}