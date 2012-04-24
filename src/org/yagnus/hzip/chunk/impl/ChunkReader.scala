package org.yagnus.hzip.chunk.impl;

import scala.Option.option2Iterable
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.lib.CombineFileInputFormat
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.RecordReader
import org.slf4j.LoggerFactory
import org.yagnus.hzip.RuntimeConstants
import org.yagnus.yadoop.Yadoop._
import org.yagnus.hzip.chunk.ChunkIOs._
import org.yagnus.hzip.chunk._
import org.yagnus.yadoop.HdfsUtils
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import scala.reflect.BeanProperty
import org.yagnus.hzip.CompressionMR
import org.yagnus.hzip.HZLogger
import org.yagnus.yadoop.io.Versioned

/**
  * This class takes arbitrary piece specifications from the job conf, and applies it to the
  * file that it is given to create the right Piece of Collections
  *
  * @author hc.busy
  *
  */
class ChunkReader extends RecordReader[SubChunk, BytesWritable]
    with CompressionMR[SubChunk, BytesWritable, SubChunk, BytesWritable] with HZLogger with Versioned {
    addVersion(1);

    private lazy implicit final val LOG_AS = initLogger(classOf[ChunkReader])
    def close {};

    def initialize(mySplit: InputSplit, context: TaskAttemptContext) {
        var conf = context.getConfiguration;
        fs = FileSystem.get(conf);

        super.configure(conf); //set up the chunks

        info("The split class is " + mySplit.getClass())
        val split = mySplit.asInstanceOf[FileSplit];
        val startInFile = split.getStart;
        info("The split length is " + split.getLength())
        val lengthInFile: Int = split.getLength.intValue;
        val splitPath = split.getPath;
        //    logger.info("The Path is " + splitPath);

        val fn = splitPath.toUri.getPath;
        //    logger.info("Checking fn=" + fn);
        val results = chunks(fn);

        info("there were " + results.length + " results:" + results);
        //assuming chunks(fn) did the right thing by returning a SubChunk's from the same Chunk
        chunkBuffer = results.map(x => { new SubChunk(x.chunkId, 0, x.getFn, startInFile, lengthInFile) & x }).flatten.iterator;

    }

    protected var chunkBuffer: Iterator[SubChunk] = null;

    def getProgress: Float = 1;

    val key: SubChunk = new SubChunk;
    val data: BytesWritable = new BytesWritable;
    def nextKeyValue(): Boolean = {
        //    logger.info("next has been called." + chunkBuffer.hasNext);
        if (!chunkBuffer.hasNext) return false;
        val sc = chunkBuffer.next;
        val file = fs.open(sc.getFn());
        val len = sc.getLength.intValue;
        val sta = sc.getStart.intValue;
        val result = new Array[Byte](len);
        file.readFully(sta, result);
        data.setCapacity(0); data.setCapacity(len);
        data.set(result, 0, len);
        key.set(sc);
        true;
    }

    override def getCurrentKey: SubChunk = key;
    override def getCurrentValue: BytesWritable = data;

}

class ChunkInputFormat extends FileInputFormat[SubChunk, BytesWritable] {
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[SubChunk, BytesWritable] = {
        val ret = new ChunkReader;
        ret.initialize(split, context);
        return ret;
    }
}
