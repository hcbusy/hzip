package org.yagnus.hzip.thirdParty.algorithms;

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.yagnus.hzip.algorithms.Algorithms.hbzip;
import org.yagnus.hzip.pieces._;
import org.yagnus.yadoop.Yadoop._
import java.io.OutputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;

/**
 * The Bzip2 compressor extends PieceCreator by adding compression in the last reduce phase
 * 
 * @author hc.busy
 *
 */
class HBzip2Compression(fs : FileSystem, gFiles : Seq[String], ofn : String, recursive : Boolean)
    extends PieceCreator(fs, gFiles, ofn, recursive,hbzip) {

    override def reducerClass = classOf[HBzip2CompressionReducer];

}

class HBzip2CompressionReducer extends PieceReducer((os : OutputStream) ⇒ new CBZip2OutputStream(os, 9));