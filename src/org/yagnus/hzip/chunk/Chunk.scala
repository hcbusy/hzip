package org.yagnus.hzip.chunk;

import scala.collection.mutable.HashMap
import scala.reflect.BeanProperty
import org.yagnus.hzip.io.{ HZVer, HZIO }
import org.slf4j.LoggerFactory

/**
 * A chunk is a piece of data that chunk compressors will operate on.
 * They are a collection of pieces from files
 */
class Chunks extends HZVer {
    //this class is just here to get serialization .
    @BeanProperty
    var chunks: List[Chunk] = List[Chunk]();

    @BeanProperty
    var fnLookup: HashMap[String, List[SubChunk]] = new HashMap[String, List[SubChunk]]();

    //Using java.lang.Long because of some weird serialization error protostuff can't serialize scala Long
    @BeanProperty
    var cidLookup: HashMap[java.lang.Long, Chunk] = new HashMap[java.lang.Long, Chunk]();

    def add(c: Chunk) {
        cidLookup(c.getChunkId) = c;
        chunks :+= c;
        for (sc <- c.getSubChunks()) add(sc);
    }

    def add(sc: SubChunk*) {
        for (subC <- sc)
            fnLookup.put(subC.getFn(), fnLookup.getOrElse(subC.getFn(), List[SubChunk]()) :+ subC);
    }

    def getFileCount: Long = fnLookup.size;
    def getFiles: Iterable[String] = fnLookup.keys;

    def apply(fn: String): Seq[SubChunk] = fnLookup.getOrElse(fn, Seq());
    def getSubChunks(fn: String): Seq[SubChunk] = apply(fn);

    def getChunk(id: Long): Chunk = cidLookup(id);
    def apply(id: Long): Chunk = cidLookup.getOrElse(id, null);

    def getFileNames = fnLookup.keySet.iterator;
    override def equals(o: Any): Boolean = {
        if (o == null) return false;
        if (!o.isInstanceOf[Chunks]) return false;
        val O: Chunks = o.asInstanceOf[Chunks];
        if (!chunks.equals(O.chunks)) return false;
        if (!fnLookup.equals(O.fnLookup)) return false;
        if (!cidLookup.equals(O.cidLookup)) return false;
        return true;
    }

}

class Chunk extends HZVer with Ordered[Chunk] {
    addVersion("1");
    def this(chunkId: Long) {
        this();
        setChunkId(chunkId);
    }

    def this(chunkId: Long, len: Long) {
        this();
        setChunkId(chunkId);
        setLength(len);
    }
    def this(chunk: Chunk) {
        this(chunk.getChunkId, chunk.getLength);
        this.setVersion(chunk.getVersion());
        for (subc <- chunk.getSubChunks) addSubChunk(new SubChunk(subc));
    }

    @BeanProperty
    var chunkId: Long = _;

    @BeanProperty
    var length: Long = _;

    @BeanProperty
    var subChunks: List[SubChunk] = List[SubChunk]();
    def addSubChunk(sc: SubChunk) {
        subChunks :+= sc;
        sc.setChunkId(getChunkId);
    }

    override def toString = "Chunk(id=" + chunkId + "){length=" + length + ", subChunks=" + subChunks + "}";

    override def equals(o: Any): Boolean = {
        if (o == null) return false;
        if (!o.isInstanceOf[Chunk]) return false;
        val O: Chunk = o.asInstanceOf[Chunk];
        if (chunkId != O.chunkId) return false;
        if (length != O.length) return false;
        if (!subChunks.sameElements(O.subChunks)) return false;
        return true;
    }

    def compare(o: Chunk): Int = {
        if (getChunkId > o.getChunkId) return 1;
        if (getChunkId == o.getChunkId) return 0;
        return -1;
    }

}

class SubChunk extends Ordered[SubChunk] with HZVer {
    private val logger = LoggerFactory.getLogger(classOf[SubChunk]);

    setVersion("1")
    def this(chunkId: Long, offset: Long, fn: String, start: Long, length: Long) {
        this();
        set(chunkId, offset, fn, start, length);
    }
    def set(chunkId: Long, offset: Long, fn: String, start: Long, length: Long) {
        this.setChunkId(chunkId);
        this.setOffset(offset);
        this.setFn(fn);
        this.setStart(start);
        this.setLength(length);
    }
    def set(o: SubChunk) {
        set(o.getChunkId, o.getOffset, o.getFn, o.getStart, o.getLength);
    }
    def this(o: SubChunk) {
        this();
        set(o);
    }

    @BeanProperty
    var chunkId: Long = _;

    @BeanProperty
    var offset: Long = _;

    @BeanProperty
    var fn: String = _;

    @BeanProperty
    var start: Long = _;

    @BeanProperty
    var length: Long = _;

    def compare(o: SubChunk): Int = {
        if (o == null) return -1;
        if (chunkId == o.getChunkId) {
            if (fn == o.getFn) {
                if (getStart == o.getStart) return 0;
                if (getStart > o.getStart) return 1;
                return -1;
            } else {
                if (fn > o.getFn) return 1
                return -1;
            }
        } else {
            if (getChunkId > o.getChunkId) return 1
            return -1;
        }
    }

    //@Returns the intersection of this and other subchunk, null if not same file or no intersection exists
    def &(o: SubChunk): Option[SubChunk] = {

        logger.info("Checking chunks " + this + " and " + o);
        if (getFn != o.getFn || getChunkId != o.getChunkId) {
            logger.error("Merge on SubChunks of different chunks or files occured." + getFn + "," + getChunkId + " versus " + o.getFn + "," + o.getChunkId);
            return None;
        }
        if (o == null) {
            logger.warn("Merge on a null and non-null chunk, returning nothing.");
            return None;
        }
        var earlier = this;
        var latter = o;
        if (getStart >= o.getStart) {
            earlier = o;
            latter = this;
        }
        if (earlier.getStart + earlier.getLength() > latter.getStart) { //There is an intersection
            val intersection = new SubChunk(earlier);
            intersection.setOffset(earlier.getOffset + latter.getStart - earlier.getStart);
            intersection.setStart(latter.getStart);
            val end = Math.min(earlier.getStart + earlier.getLength, latter.getStart + latter.getLength);
            intersection.setLength(end - latter.getStart);
            return Some(intersection);
        }
        return None;
    }

    override def equals(o: Any): Boolean = {
        if (o == null) return false;
        if (!o.isInstanceOf[SubChunk]) return false;
        val O: SubChunk = o.asInstanceOf[SubChunk];
        if (!chunkId.equals(O.chunkId)) return false;
        if (!offset.equals(O.offset)) return false;
        if (!fn.equals(O.fn)) return false;
        if (!start.equals(O.start)) return false;
        if (!length.equals(O.length)) return false;
        return true;
    }
    override def toString: String = {
        return "SubChunk{id=" + chunkId + ",fn=" + fn + ",offset=" + offset + ",start=" + start + ",length=" + length + "}";
    }
}
