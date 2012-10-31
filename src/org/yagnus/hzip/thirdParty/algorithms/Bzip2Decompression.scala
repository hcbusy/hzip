package org.yagnus.hzip.thirdParty.algorithms
import java.io.InputStream
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.mapred._
import org.yagnus.hzip.algorithms._
import org.yagnus.yadoop._
import org.yagnus.yadoop.Yadoop._

/**
 * The Gzip decompressor sends to mapreduce (input.gz,outputfn) pairs, which it expects the map phase to decompress.
 * 
 * @author hc.busy
 *
 */
class Bzip2Decompression(fs : FileSystem, gFiles : Seq[Path], oFile : Seq[String], globalTempDir : String)
		extends FileNameDecompressionAlgorithm(fs, gFiles, oFile, globalTempDir) {

	def mapperClass : java.lang.Class[_ <: Mapper[_, _, _, _]] = classOf[Bzip2DecompressionMapper];
	
}

class Bzip2DecompressionMapper extends FileNameDecompressionMapper((inputStream : InputStream) ⇒ new BZip2CompressorInputStream(inputStream));

