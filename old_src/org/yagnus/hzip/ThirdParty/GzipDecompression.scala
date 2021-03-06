package org.yagnus.hzip.ThirdParty

import org.yagnus.hzip.{ FileNameDecompressionAlgorithm, FileNameDecompressionMapper, CompressionJob, Common };
import org.yagnus.yadoop._;
import org.yagnus.yadoop.Yadoop._;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred._;

import scala.collection.mutable.ArrayBuffer;
import scala.util.Random;

import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs._
import org.apache.hadoop.io._


/**
 * The Gzip decompressor sends to mapreduce (input.gz,outputfn) pairs, which it expects the map phase to decompress.
 * 
 * @author hc.busy
 *
 */
class GzipDecompression(fs : FileSystem, gFiles : Seq[Path], oFile : Seq[String], globalTempDir : String)
		extends FileNameDecompressionAlgorithm(fs, gFiles, oFile, globalTempDir) {

	def mapperClass : java.lang.Class[_ <: Mapper[_, _, _, _]] = classOf[GzipDecompressionMapper];
}

class GzipDecompressionMapper extends FileNameDecompressionMapper((inputStream : InputStream) ⇒ new GZIPInputStream(inputStream));