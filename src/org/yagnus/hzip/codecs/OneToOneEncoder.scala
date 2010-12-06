package org.yagnus.hzip.codecs

trait OneToOneEncoder[In,Out] extends Function1[In, Out]{

	def encodeOneSymbol(in:In):Out;
	
	def apply(in:In):Out = encodeOneSymbol(in);
}
