package org.yagnus.hzip.bits {

    import java.util.BitSet;

    /**
     * Bit is a bit, on or off, apply returns the value of the bit
     * @author hsb
     *
     */
    abstract class Bit {
        def apply() : Boolean;
    }

    abstract class MutableBit extends Bit {
        def set(value : Boolean);
    }

    object CONSTANTS {
        final val on = new Bit() {
            final override def apply() : Boolean = true;
            final override def toString() : String = "1";
        }
        final val off = new Bit() {
            final override def apply() : Boolean = false;
            final override def toString() : String = "0";
        }

        final implicit def bitToBoolean(bit : Bit) : Boolean = bit();
        final implicit def booleanToBit(b : Boolean) : Bit = if (b) on else off;

        final implicit def bitItrToBoolean(bits : Iterator[Bit]) : Iterator[Boolean] = for (b ← bits) yield b();
        final implicit def booleanItrToBit(bools : Iterator[Boolean]) : Iterator[Bit] = for (b ← bools) yield if (b) on else off;

        final implicit def bitItrblToBoolean(bits : Iterable[Bit]) : Iterable[Boolean] = new Iterable[Boolean]() {
        	override def iterator:Iterator[Boolean] = bits.iterator;
        };
        final implicit def booleanItrbleToBit(bools : Iterable[Boolean]) : Iterable[Bit] =new Iterable[Bit]() {
        	override def iterator:Iterator[Bit] = bools.iterator;
        }

        def getBit(l : Long, ind : Int) : Bit = {
            if (((l >> ind) & 1l) > 0) return on;
            else return off;
        }

        def getBit(i : Int, ind : Int) : Bit = {
            if (((i >> ind) & 1) > 0) return on;
            else return off;
        }

        def getBit(s : Short, ind : Int) : Bit = {
            if (((s >> ind) & 1) > 0) return on;
            else return off;
        }

        def getBit(b : Byte, ind : Int) : Bit = {
            if (((b >> ind) & 1) > 0) return on;
            else return off;
        }

        def getBit(bs : BitSet, index : Int) : Bit = {
            if (bs.get(index)) return on;
            else return off;
        }
    }
}