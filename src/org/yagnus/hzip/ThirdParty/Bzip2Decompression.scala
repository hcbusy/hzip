package org.yagnus.hzip.ThirdParty
import org.yagnus.hzip.{ HadoopCompressionAlgorithm, CompressionJob, Common };
import org.yagnus.yadoop._;
import org.yagnus.yadoop.Yadoop._;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred._;

import scala.collection.mutable.ArrayBuffer;
import scala.util.Random;

import java.io.InputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream; 

import org.apache.hadoop.fs._
import org.apache.hadoop.io._

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

class Bzip2DecompressionMapper extends FileNameDecompressionMapper((inputStream : InputStream) ⇒ new CBZip2InputStream(inputStream));

