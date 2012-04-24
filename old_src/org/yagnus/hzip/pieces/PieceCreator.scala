package org.yagnus.hzip.pieces

import org.yagnus.hzip.{ HZLogger, CompressionBase, Common, MetaData }
import org.yagnus.hzip.Common.INTERNAL_JOBCONF_PIECE_SPEC;
import org.yagnus.hzip.HdfsUtils;
import org.yagnus.hzip.algorithms._;

import org.yagnus.scalasupport.RichArrays._;
import org.yagnus.yadoop.LongArrayWritable;
import org.yagnus.yadoop.Yadoop._;

import java.io.{ DataInput, DataOutput }
import java.util.Iterator
import java.io.{ OutputStream, ByteArrayOutputStream };

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import scala.collection.mutable.{ HashMap, ArrayBuffer };

/**
 * 
 * The piece creator is an algorithm that takes input files split it into pieces so that subsequent block-wise compression 
 * algorithms can perform compression on these pieces. <p />
 * 
 * The user should derive from PieceCreator and specify a reducerClass.
 * 
 * The reducer class should perform the compression on the piece (block) it receives. The default {@link PieceReducer} allows
 * you to derive from it and add a compressor and perform stream compression on each block before writing them out. 
 * 
 *  @author hc.busy
 *  
 *  @param fs The file system
 *  @param inFiles the files or paths to scan as compression candidates
 *  @param outputDir the directory to output to
 *  @param recursive whether to scan the inFiles recursively.
 */
class PieceCreator(fs : FileSystem, inFiles : Seq[String], var outputDir : String, pathFilter : PathFilter, recursive : Boolean)
    extends HadoopCompressionAlgorithm(fs) {

    private lazy val _l = new HZLogger(classOf[PieceCreator]);

    def this(fs : FileSystem, inFiles : Seq[String], outputDir : String, recursive : Boolean, algo : Algorithms.Algorithms) {
        this(fs, inFiles, outputDir, new PathFilter { override def accept(p : Path) : Boolean = true }, recursive);
        metaData.algorithm = algo;
    }

    val metaData : MetaData = new MetaData();

    def reducerClass : java.lang.Class[_ <: Reducer[_, _, _, _]] = classOf[PieceReducer];

    def createCompressionJob() : CompressionJob = {
        val jb = new JobConf();

        val pieceMaker = new FilePieceMaker();
        val (inputDirectories, inputFileStata) = HdfsUtils.getFileDirStatus(fs, inFiles, pathFilter, recursive, pieceMaker);

        // Here are all the files from the recursive scan of directories
        var fns = inputFileStata.map(_.getPath.toUri.getPath);
        jb.setStrings("mapred.input.dir", fns.toArray[String] : _*);
        if (!outputDir.endsWith("/")) outputDir += "/";
        FileOutputFormat.setOutputPath(jb, outputDir + Common.OUTPUT_HZ_DATA_FILE);
        jb.setStrings(Common.CONFIG_OUTPUT_PATH, outputDir);

        jb.setMapperClass(classOf[PieceMapper]);
        jb.setReducerClass(reducerClass);

        jb.setMapOutputKeyClass(classOf[LongArrayWritable]);
        jb.setMapOutputValueClass(classOf[IntermediatePiece]);

        jb.setInputFormat(classOf[PieceInputFormat]);
        jb.setOutputFormat(classOf[SequenceFileOutputFormat[LongArrayWritable, BytesWritable]]);

        jb.setOutputKeyClass(classOf[LongArrayWritable]);
        jb.setOutputValueClass(classOf[BytesWritable]);
        //There is no combiners for now

        jb.setPartitionerClass(classOf[PiecePartitioner]);

        jb.setOutputKeyComparatorClass(classOf[PieceSortingComprator]);
        jb.setOutputValueGroupingComparator(classOf[PieceGroupingComprator]);

        //set the tasks so that each batch is dumped to a single file.
        jb.setNumReduceTasks(pieceMaker.getKeyCount.intValue + 2);

        jb.setJarByClass(classOf[PieceReducer]);

        jb.setCompressMapOutput(false)

        //finally add the batchspec to the configurations.
        //TODO: later migrate the batch spec to an HDFS file in case we are archiving a large number of billions of files...
        jb.setStrings(INTERNAL_JOBCONF_PIECE_SPEC, pieceMaker.toConfigString);
        //return (jb, pieceMaker);
        val ret = new CompressionJob(jb) {
            override def postHadoop() = {
                //write metadata out.
                //                _l.error("Why is the algorithm not specifiable?"+metaData.algorithm);
                MetaData.writeData(fs, outputDir + Common.OUTPUT_HZ_META_DATA, metaData);
                FilePieceMaker.writeData(fs, outputDir + Common.OUTPUT_HZ_MANIFEST_FILE, pieceMaker);
            }
        }
        return ret;
    }
}

/**
 * This class represents a piece of the piece in transit between map and reduce
 * 
 */
class IntermediatePiece extends Writable {
    def this(cn : Long, pn : Long, sb : Long, bl : Long, dt : BytesWritable) {
        this();
        collectioNumber = cn;
        pieceNumber = pn;
        startingByte = sb;
        byteLength = bl;
        data = dt;
    }

    var collectioNumber : Long = 0;
    var pieceNumber : Long = 0;
    var startingByte : Long = 0;
    var byteLength : Long = 0;
    var data = new BytesWritable();

    override def readFields(in : DataInput) {
        collectioNumber = in.readLong;
        pieceNumber = in.readLong;
        startingByte = in.readLong;
        byteLength = in.readLong;
        data.readFields(in);
    }

    override def write(out : DataOutput) {
        out.writeLong(collectioNumber);
        out.writeLong(pieceNumber);
        out.writeLong(startingByte);
        out.writeLong(byteLength);
        data.write(out);
    }

}

class PieceMapper extends CompressionBase
    with Mapper[LongArrayWritable, BytesWritable, LongArrayWritable, IntermediatePiece] {
    private lazy val _l = new HZLogger(classOf[PieceMapper]);

    def map(key : LongArrayWritable, value : BytesWritable,
        output : OutputCollector[LongArrayWritable, IntermediatePiece], reporter : Reporter) {

        val arr = key.get;
        val piece = new IntermediatePiece(arr(0), arr(1), arr(2), arr(3), value);

        //        _l.error("PieceMapper has length "+value.getLength+", bytelength is "+piece.byteLength+" writing a piece '"+(new String(value))+"'");
        output.collect(key, piece);
    }
}

/**
 * This is a reference implementation of PieceReducer. In this case, deriving from this class requires that you provide
 * a stream compressor on the parameter. The default compression operation is an noop operation which outputs the data
 * without changing it.
 *  
 *  TODO: make the compression contiguous instead of starting a new stream every block
 *  
 * @author hc.busy
 *
 */
class PieceReducer(compressTo : OutputStream ⇒ OutputStream) extends CompressionBase
    with Reducer[LongArrayWritable, IntermediatePiece, LongArrayWritable, BytesWritable] {

    def this() {
        this((x : OutputStream) ⇒ x);
    }

    private lazy val _l = new HZLogger(classOf[PieceMapper]);

    var blockSize = 5 * 64 * 1024 * 1024; //defaults to 320 megabytes

    def reduce(key : LongArrayWritable, values : Iterator[IntermediatePiece],
        output : OutputCollector[LongArrayWritable, BytesWritable], reporter : Reporter) {

        var bufferWriter = new ByteArrayOutputStream(blockSize);
        var zippedWriter = compressTo(bufferWriter);
        var cntr : Long = 0;
        for (value ← values) {
            //            _l.error("PiceCreator reducer the inputblock is '"+(new String(value.data.exactByteArray)+"'");

            zippedWriter.write(value.data.wholeBytesBuffer, 0, value.data.getLength);
            zippedWriter.flush();
            bufferWriter.flush();

            if (bufferWriter.size > blockSize) {

                //close everything down
                zippedWriter.close();
                bufferWriter.close();

                //write it out
                val okey = new LongArrayWritable(Array(key.get(0), cntr));
                output.collect(okey, bufferWriter.toByteArray);
                cntr += 1;

                if (values.hasNext) {
                    var bufferWriter = new ByteArrayOutputStream(blockSize);
                    var zippedWriter = compressTo(bufferWriter);
                } else {
                    bufferWriter = null;
                }
            }

        }

        if (bufferWriter != null) {
            zippedWriter.close();
            bufferWriter.close();
            if (bufferWriter.size > 0) {
                val okey = new LongArrayWritable(Array(key.get(0), cntr));
                //                _l.error("PieceMapper reducer writing a piece '"+(new String(bufferWriter.toByteArray))+"'");
                output.collect(okey, bufferWriter.toByteArray);
            }
        }
    }
}

class PiecePartitioner extends Partitioner[LongArrayWritable, IntermediatePiece] {
    // The batch number is the only thing we partition by.
    def getPartition(key : LongArrayWritable, value : IntermediatePiece, numPartitions : Int) : Int =
        ((key.get(0).longValue - Integer.MIN_VALUE) % numPartitions).intValue;
    override def configure(job : JobConf) = {}
}

class PieceSortingComprator extends RawComparator[LongArrayWritable] {
    def compare(b1 : Array[Byte], s1 : Int, l1 : Int,
        b2 : Array[Byte], s2 : Int, l2 : Int) : Int = {

        return WritableComparator.compareBytes(b1, s1, 4 * java.lang.Long.SIZE / 8,
            b2, s2, 4 * java.lang.Long.SIZE / 8);
    }

    def compare(l1 : LongArrayWritable, l2 : LongArrayWritable) : Int = {
        val l : Ordered[Array[Long]] = org.yagnus.scalasupport.RichArrays.makeArrayComparable(l1.array);
        return l.compareTo(l2.array);
    }
}

class PieceGroupingComprator extends RawComparator[LongArrayWritable] {
    def compare(b1 : Array[Byte], s1 : Int, l1 : Int,
        b2 : Array[Byte], s2 : Int, l2 : Int) : Int = {

        return WritableComparator.compareBytes(b1, s1, java.lang.Long.SIZE / 8,
            b2, s2, java.lang.Long.SIZE / 8);
    }

    def compare(l1 : LongArrayWritable, l2 : LongArrayWritable) : Int = {
        val l = l1.array(0);
        val r = l2.array(0);
        if (l < r) return -1;
        if (l > r) return 1;
        return 0;
    }
}