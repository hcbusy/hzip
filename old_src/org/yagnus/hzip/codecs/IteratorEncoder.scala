
package org.yagnus.hzip.codecs;


trait IteratorEncoder[In, Out] extends Encoder[In,Out]{

	override def encodeSymbol(in:In):Iterator[Out] = Iterator.empty; // make it possible to jsut write encode;
	
	def encode[V <% Iterator[In]](ins: V):Iterator[Out]=
		(for(in<-ins) yield encodeSymbol(in)).flatten;
	
	
}

class EmptyIteratorEncoder[In,Out] extends IteratorEncoder[In, Out]{
	override def encode[V <% Iterator[In]](ins: V):Iterator[Out] = Iterator.empty;
	
}