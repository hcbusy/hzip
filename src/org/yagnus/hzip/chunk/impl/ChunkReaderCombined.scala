//package org.yagnus.hzip.chunk;
//
//import scala.Option.option2Iterable
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.io.BytesWritable
//import org.apache.hadoop.mapred.lib.CombineFileInputFormat
//import org.apache.hadoop.mapred.lib.CombineFileSplit
//import org.apache.hadoop.mapred.JobConf
//import org.apache.hadoop.mapreduce.RecordReader
//import org.slf4j.LoggerFactory
//import org.yagnus.hzip.RuntimeConstants
//import org.yagnus.yadoop.Yadoop.__string2path
//import org.yagnus.hzip.chunk.ChunkIOs._
//import org.yagnus.yadoop.HdfsUtils
//import org.apache.hadoop.mapred.Reporter
//import org.apache.hadoop.mapreduce.InputSplit
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
//import org.apache.hadoop.mapreduce.TaskAttemptContext
//import org.apache.hadoop.conf.Configuration
//
///**
// * These classes are remains from Hzip2011 using the old hadoop mapred.* classes
// */
//class ChunkReaderCombined(mySplit: CombineFileSplit, jobConf: JobConf) extends RecordReader[SubChunk, BytesWritable] {
//
//  private lazy val logger = LoggerFactory.getLogger(classOf[ChunkReader])
//
//  val fs: FileSystem = FileSystem.get(jobConf);
//  var chunks: Chunks = null;
//
//  def initChunks {
//    //This initialization may be too much for jvm to perform at load time since it has to load other classes
//    //so delay the initialization until later.
//    if (chunks == null) {
//      //    new ChunksIO().deserializeFromBytes(HdfsUtils.readFully(fs, jobConf.getStrings(RuntimeConstants.CHUNK_FILE_NAME)(0)));
//      val data = HdfsUtils.readFully(fs, jobConf.getStrings(RuntimeConstants.CHUNK_FILE_NAME)(0));
//      logger.error("the file path is" + jobConf.getStrings(RuntimeConstants.CHUNK_FILE_NAME)(0));
//      logger.error("The data length is :" + data.length);
//      logger.error("The length of the file from status is:" + fs.getFileStatus(new Path(jobConf.getStrings(RuntimeConstants.CHUNK_FILE_NAME)(0))).getLen());
//      logger.error("The chunk data is:'" + new String(data) + "'");
//      logger.error("The Chunks IO objct is " + (new ChunksIO));
//
//      //    new ChunksIO().deserializeOutOfBytes(data);
//      chunks = new ChunksIO().deserializeFromBytes(data);
//      logger.error("The read data reserialized is " + chunks.getFnLookup);
//    }
//  }
//
//  protected var chunkBuffer: Iterator[SubChunk] = null;
//  def getChunkBufferItr: Iterator[SubChunk] = {
//    if (chunkBuffer == null) {
//      chunkBuffer = (for (i <- 0 until mySplit.getNumPaths()) yield {
//        val offset = mySplit.getOffset(i);
//        val len: Int = mySplit.getLength(i).intValue;
//        val splitPath = mySplit.getPath(i);
//        logger.info("The Path is " + splitPath);
//
//        val url = splitPath.toUri();
//        logger.info("The url is " + url);
//
//        val fn = url.getPath;
//        logger.info("The fn=" + fn);
//        val results = chunks(fn);
//
//        logger.info("there were " + results.length + " results:" + results);
//        results.map(new SubChunk(-1, 0, fn, offset, len) & _);
//      }).flatten.flatten.iterator;
//    }
//    return chunkBuffer;
//  }
//
//  def getProgress: Float = 1;
//
//  def nextKeyValue(key: SubChunk, data: BytesWritable): Boolean = {
//    initChunks;
//    if (!getChunkBufferItr.hasNext) return false;
//    val sc = getChunkBufferItr.next;
//    val file = fs.open(sc.getFn());
//    val len = sc.getLength.intValue;
//    val sta = sc.getStart.intValue;
//    val result = new Array[Byte](len);
//    file.readFully(sta, result);
//    data.setCapacity(0); data.setCapacity(len);
//    data.set(result, 0, len);
//    key.set(sc);
//    true;
//  }
//
//  override def getCurrentKey: SubChunk = new SubChunk();
//  override def getCurrentValue: BytesWritable = new BytesWritable();
//
//}
//
//class ChunkInputFormatCombined extends CombineFileInputFormat[SubChunk, BytesWritable] {
//  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[SubChunk, BytesWritable] = {
//    val actualSplits = split.asInstanceOf[CombineFileSplit];
//    return new ChunkReaderCombined(actualSplits, job);
//  }
//}
