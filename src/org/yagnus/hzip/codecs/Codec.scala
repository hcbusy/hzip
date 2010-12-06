package org.yagnus.hzip.codecs
/**
 * In hzip, a codec is a streaming codec that encodes the input with just one pass
 * 
 * For now, the iterators should be implemented lazy. (And all system can depend on the fact that it is lazy)
 * when an iterator is created:
 * 
 *   i = codec.decode(x);
 *   j = codec.decode(x);
 * 
 * it is expected that i and j are able to share the one iterator. i.next will read as many entries from x.next as it needs, and then stop. This way j is able to pick up in the stream where i left off.
 *
 *There are several ways to implement this interface. One can implement the "decode" and "encoder" methods to return the appropriate IteratorEncoder.
 *  One can also directly override the "encode" and "decode" methods to operate directly on the symbol iterators. Since "encoder" and "decoder" are protected,
 *  they are not accessible to the outside and are only provided for classes that can implement that.
 *
 *  the "encode" and "decode" that operate on Iterable's should not be overriden, but should be the method to use, since it is lazy.
 * 
 * @author hsb
 * 
 **/

trait Codec[In, Out] {

    protected def encoder : IteratorEncoder[In, Out] = null; // generates runtime exception if invoked
    protected def decoder : IteratorEncoder[Out, In] = null; // "

    def encode(input : Iterator[In]) : Iterator[Out] =
        encoder.encode(input);

    def decode(input : Iterator[Out]) : Iterator[In] =
        decoder.encode(input);

    def encode[V <% Iterable[In]](input : V) : Iterable[Out] = new Iterable[Out] {
        def iterator = encoder.encode(input.iterator);
    }

    def decode[V <% Iterable[Out]](input : V) : Iterable[In] = new Iterable[In] {
        def iterator = decoder.encode(input.iterator);
    }

    def encodeSymbol(input : In) : Iterator[Out] =
        encoder.encodeSymbol(input);

}
