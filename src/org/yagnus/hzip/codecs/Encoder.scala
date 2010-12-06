package org.yagnus.hzip.codecs

trait Encoder[In, Out] {
	def encodeSymbol(in:In):Iterator[Out];
}
