package org.yagnus.hzip.codecs

trait OneToOneCodec[In, Out] extends OneToOneEncoder[In, Out] {

    protected def symbolEncoder : OneToOneEncoder[In, Out] = null;
    protected def symbolDecoder : OneToOneEncoder[Out, In] = null;

    def encodeOneSymbol(in : In) : Out = symbolEncoder.encodeOneSymbol(in);
    def decodeOneSymbol(in : Out) : In = symbolDecoder.encodeOneSymbol(in);

}

/**
 * This wrapper makes a one-to-one codec into a normal streaming Codec
 * 
 * One will have to implement class extends OneToOneCodec with ManyToManyCodec
 * and then define the method that gets the underlyingOneToOneCodec
 * 
 * @author hc.busy
 *
 */
trait OneToOneToManyToManyCodec[In, Out] extends Codec[In, Out] with OneToOneCodec[In,Out]{

    override protected def encoder = new IteratorEncoder[In, Out] {
        override def encodeSymbol(in : In) = Iterator(encodeOneSymbol(in));
    }

    override protected def decoder = new IteratorEncoder[Out, In] {
        override def encodeSymbol(in : Out) = Iterator(decodeOneSymbol(in));
    }
}
