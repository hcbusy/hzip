package org.yagnus.hzip

import org.yagnus.hzip.Common._;

import org.yagnus.yadoop.LongArrayWritable;
import org.yagnus.yadoop.Yadoop._;
import org.yagnus.hzip.pieces.Piece;

import java.io.{ DataInput, DataOutput }
import java.util.Iterator
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import scala.collection.mutable.{ HashMap, ArrayBuffer };

class CompressionBase extends MapReduceBase {
    var outputPath : String = null;
    var fs : FileSystem = null;
    var tempDir : String = null;
    var pieceSpec:Piece=null;

    override def configure(job : JobConf) = {
    	fs = FileSystem.get(job);
    	outputPath = job.getStrings(Common.CONFIG_OUTPUT_PATH)(0);
    }
}