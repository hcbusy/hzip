package org.yagnus.hzip.pieces

import org.yagnus.hzip.HZLogger;

import org.yagnus.hzip.Common;

import org.yagnus.yadoop._;
import org.yagnus.yadoop.Yadoop._;

//TODO: this file is all stubs for now
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.lib.{ CombineFileInputFormat, CombineFileSplit };
import scala.collection.mutable.LinkedList;
import scala.math.{ min, max };

/**
 * This class takes arbitrary piece specifications from the job conf, and applies it to the 
 * file that it is given to create the right Piece of Collections
 * 
 * @author hc.busy
 *
 */
class PieceRecordReader(mySplit : CombineFileSplit, jobConf : JobConf) extends RecordReader[LongArrayWritable, BytesWritable] {

    private lazy val _l = new HZLogger(classOf[PieceRecordReader]);

    val fs = FileSystem.get(jobConf);
    var pieceSpec : FilePieceMaker = null;
    def updatePieceSpec() {
        if (pieceSpec == null)
            this.synchronized {
                if (pieceSpec == null)
                    pieceSpec = new FilePieceMaker(jobConf.getStrings(Common.INTERNAL_JOBCONF_PIECE_SPEC)(0));
            }
    }

    def close(reporter : Reporter) = {}
    def close() = {};
    def getProgress : Float = 1;
    def getPos : Long = 0;

    var curPos = 0;

    def next(key : LongArrayWritable, data : BytesWritable) : Boolean = {

        updatePieceSpec();
        if (curPos < mySplit.getNumPaths) {

            val offset = mySplit.getOffset(curPos);
            val len : Int = mySplit.getLengths()(curPos).intValue;
            val fn = mySplit.getPath(curPos).toUri.getPath;

            val file = fs.open(fn);
            val result = new Array[Byte](len);

            file.readFully(offset, result);
            val ret = pieceSpec.getPieces(fn) match {
                case None ⇒ false;
                case Some(pieces) ⇒
                    for (piece ← pieces) {
                        val pieceNumber = pieceSpec.getPieces(fn) match { case Some(x) ⇒ x; case None ⇒ -1; }
                        key.set(Array[Long](piece.collectionId, piece.id, offset, len)); // these are in order of IntermediatePiece constructor parameter
                        //                        println("Reader outputing some data of length "+len+" '"+(new String(result.slice(0, len)))+"'");
                        data.setCapacity(0); data.setCapacity(len);
                        data.set(result, 0, len);
                    }
                    true
            }

            curPos += 1;
            return ret;
        } else {
            return false;
        }
    }

    def createKey : LongArrayWritable = new LongArrayWritable();
    def createValue : BytesWritable = new BytesWritable();

}

class PieceInputFormat extends CombineFileInputFormat[LongArrayWritable, BytesWritable] {
    override def getRecordReader(split : InputSplit, job : JobConf, reporter : Reporter) : RecordReader[LongArrayWritable, BytesWritable] = {
        val actualSplits = split.asInstanceOf[CombineFileSplit];
        return new PieceRecordReader(actualSplits, job);
    }
}
