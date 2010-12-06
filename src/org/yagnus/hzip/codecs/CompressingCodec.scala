/**
  * codecs that output less bits than going in can be marked with this trait
 **/
trait CompressingCodec{
	def compressionRate:Double;
}
