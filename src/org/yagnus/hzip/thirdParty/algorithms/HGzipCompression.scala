package org.yagnus.hzip.thirdParty.algorithms
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.yagnus.hzip.algorithms.Algorithms.hgzip;
import org.yagnus.hzip.pieces._;
import org.yagnus.yadoop.Yadoop._
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.yagnus.hzip.algorithms._;
import org.yagnus.hzip.pieces._;

/**
 * The Gzip compressor extends PieceCreator by adding compression in the last reduce phase
 * 
 * @author hc.busy
 *
 */
class HGzipCompression(fs : FileSystem, gFiles : Seq[FileStatus], gDirs:Seq[FileStatus],ofn : String, pieceMaker:FilePieceMaker)
    extends PieceCreator(fs, gFiles, gDirs, ofn, hgzip, pieceMaker) {

    override def reducerClass = classOf[HGzipCompressionReducer];

}

class HGzipCompressionReducer extends PieceReducer((os : OutputStream) ⇒ new GZIPOutputStream(os));