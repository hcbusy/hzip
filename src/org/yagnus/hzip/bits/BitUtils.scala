package org.yagnus.hzip.bits;

/**
 * This object helps us to convert primitive multi-byte objects into bytes and back.
 *
 * @author hsb
 *
 */
object BitUtils {

    // long
    def bytes2long(a : IndexedSeq[Byte]) : Long = bytes2long(a, 0);
    def bytes2long(a : IndexedSeq[Byte], s : Int) : Long = bytes2long(a.view(s, s + 8).iterator);
    def bytes2long(a : Iterator[Byte]) : Long = {
        return ((a.next & 0xffl) << 56) |
            ((a.next & 0xffl) << 48) |
            ((a.next & 0xffl) << 40) |
            ((a.next & 0xffl) << 32) |
            ((a.next & 0xffl) << 24) |
            ((a.next & 0xffl) << 16) |
            ((a.next & 0xffl) << 8) |
            (a.next & 0xffl)
    }

    def long2bytes(l : Long) : Array[Byte] = {
        val ret = new Array[Byte](8);
        long2bytes(ret, 0, l);
        return ret;
    }

    def long2bytes(o : scala.collection.mutable.IndexedSeq[Byte], s : Int, l : Long) {
        var ii = 56;
        for (i ← 0 until 8) {
            o(i) = ((l >> ii) & 0xffl).byteValue;
            ii -= 8;
        }
    }

    //int
    def bytes2int(a : IndexedSeq[Byte]) : Int = bytes2int(a, 0);

    def bytes2int(a : IndexedSeq[Byte], s : Int) : Int = {
        return ((a(s + 0) & 0xff) << 24) |
            ((a(s + 1) & 0xff) << 16) |
            ((a(s + 2) & 0xff) << 8) |
            (a(s + 3) & 0xff)
    }

    def int2bytes(l : Int) : Array[Byte] = {
        val ret = new Array[Byte](8);
        int2bytes(ret, 0, l);
        return ret;
    }
    def int2bytes(o : scala.collection.mutable.IndexedSeq[Byte], s : Int, l : Int) {
        var ii = 24;
        for (i ← 0 until 4) {
            o(i) = ((l >> ii) & 0xff).byteValue;
            ii -= 8;
        }
    }

    //short
    private val shortMask = 0xffl.shortValue;
    def bytes2short(a : IndexedSeq[Byte]) : Short = bytes2short(a, 0);

    def bytes2short(a : IndexedSeq[Byte], s : Int) : Short = {
        return (((a(s + 0) & shortMask) << 8) | (a(s + 1) & shortMask)).shortValue;
    }

    def short2bytes(l : Short) : Array[Byte] = {
        val ret = new Array[Byte](8);
        short2bytes(ret, 0, l);
        return ret;
    }
    def short2bytes(o : scala.collection.mutable.IndexedSeq[Byte], s : Int, l : Short) {
        var ii = 8;
        for (i ← 0 until 2) {
            o(i) = ((l >> ii) & 0xff).byteValue;
            ii -= 8;
        }
    }

    def bitsPrint(in : Iterable[Bit]) : Unit = bitsPrint(in.iterator);
    def bitsPrint(in : Iterator[Bit]) {
        print("Bits{");
        for (b ← in)
            bitsPrint(b);
        println("}");
    }
    def bitsPrint(in : Bit) {
        if (in()) print("1,") else print("0,");
    }
}
