package org.yagnus.hzip.thirdParty.algorithms

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.yagnus.hzip.algorithms.Algorithms.lzma;
import net.contrapunctus.lzma.LzmaOutputStream;
import org.yagnus.hzip.pieces._;
import org.yagnus.yadoop.Yadoop._
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.yagnus.hzip.algorithms._;
import org.yagnus.hzip.pieces._;

class HLZMACompression(fs : FileSystem, gFiles : Seq[FileStatus], gDirs:Seq[FileStatus],ofn : String, pieceMaker:FilePieceMaker)
    extends PieceCreator(fs, gFiles, gDirs, ofn, lzma, pieceMaker) {

    override def reducerClass = classOf[HLZMACompressionReducer];

}

class HLZMACompressionReducer extends PieceReducer((os : OutputStream) ⇒ new LzmaOutputStream(os));