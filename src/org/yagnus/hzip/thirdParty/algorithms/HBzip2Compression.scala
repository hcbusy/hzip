package org.yagnus.hzip.thirdParty.algorithms;

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.yagnus.hzip.algorithms.Algorithms.hbzip;
import org.yagnus.hzip.pieces._;
import org.yagnus.yadoop.Yadoop._
import java.io.OutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.yagnus.hzip.algorithms._;
import org.yagnus.hzip.pieces._;

/**
 * The Bzip2 compressor extends PieceCreator by adding compression in the last reduce phase
 * 
 * @author hc.busy
 *
 */
class HBzip2Compression(fs : FileSystem, gFiles : Seq[FileStatus], gDirs : Seq[FileStatus], ofn : String, pieceMaker:FilePieceMaker)
    extends PieceCreator(fs, gFiles, gDirs, ofn, hbzip, pieceMaker) {

    override def reducerClass = classOf[HBzip2CompressionReducer];

}

class HBzip2CompressionReducer extends PieceReducer((os : OutputStream) ⇒ new BZip2CompressorOutputStream(os, 9));