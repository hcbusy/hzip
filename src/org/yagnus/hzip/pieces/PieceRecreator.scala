package org.yagnus.hzip.pieces;

import org.yagnus.hzip.{ HZLogger, CompressionMRBase, Common, MetaData }
import org.yagnus.hzip.Common.INTERNAL_JOBCONF_PIECE_SPEC;
import org.yagnus.hzip.HdfsUtils;
import org.yagnus.hzip.algorithms._;

import org.yagnus.scalasupport.RichArrays._;
import org.yagnus.yadoop.LongArrayWritable;
import org.yagnus.yadoop.Yadoop._;

import java.io.{ DataInput, DataOutput, InputStream, ByteArrayOutputStream, ByteArrayInputStream }
import java.util.Iterator

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import scala.collection.mutable.{ HashMap, ArrayBuffer };

/**
 * This class is the inverse of the {@link PieceCreator}. Implements the
 * algorithm which will take a piece created by the {@link PieceCreator} and
 * output the content of original file.
 * 
 * @author hc.busy
 * 
 * @param fs the file system to use.
 * @param source the hzip compressed .hz file.
 * @param target where to put the content of source
 * @param pathFilter a filter that takes the original fully qualified filename and return true if it wishes decompress it.
 */

class PieceRecreator(fs : FileSystem, var source : String, var outputDir : String, pathFilter : PathFilter) extends HadoopCompressionAlgorithm(fs) {

    val metaData : MetaData = new MetaData;

    def mapperClass : java.lang.Class[_ <: Mapper[_, _, _, _]] = classOf[PieceRecreatorMapper];

    /**
     * A permissive constructor that allows a files to be included
     */
    def this(fs : FileSystem, source : String, outputDir : String) {
        this(fs, source, outputDir, new PathFilter { override def accept(p : Path) : Boolean = true });
    }

    def createCompressionJob() : CompressionJob =
        {
            if (!source.endsWith("/")) source += "/";
            if (!outputDir.endsWith("/")) outputDir += "/";

            val jb = new JobConf();

            //Read in the metadata
            val pieceMaker = FilePieceMaker.readData(fs, source + Common.OUTPUT_HZ_MANIFEST_FILE) match {
                case None ⇒
                    println("Could not find the meta.manifest file in "+source);
                    return null;
                case Some(x) ⇒ x;
            }
            val metaData = MetaData.readData(fs, source + Common.OUTPUT_HZ_META_DATA) match {
                case None ⇒
                    println("Could not find the meta.data file in "+source);
                    return null;
                case Some(x) ⇒ x;
            }

            println("setting source to "+source + Common.OUTPUT_HZ_DATA_FILE);
            jb.setStrings("mapred.input.dir", source + Common.OUTPUT_HZ_DATA_FILE);
            FileInputFormat.setInputPaths(jb, source + Common.OUTPUT_HZ_DATA_FILE);
            if (!outputDir.endsWith("/")) outputDir += "/";
            FileOutputFormat.setOutputPath(jb, outputDir + Common.LOCAL_TEMP);
            jb.setStrings(Common.CONFIG_OUTPUT_PATH, outputDir);

            jb.setMapperClass(mapperClass);
            jb.setReducerClass(classOf[PieceRecreatorReducer]);

            jb.setMapOutputKeyClass(classOf[IntermediateFileIndex]);
            jb.setMapOutputValueClass(classOf[BytesWritable]);

            jb.setInputFormat(classOf[SequenceFileInputFormat[LongArrayWritable, BytesWritable]]);
            jb.setOutputFormat(classOf[SequenceFileOutputFormat[LongArrayWritable, BytesWritable]]);

            jb.setOutputKeyClass(classOf[LongArrayWritable]);
            jb.setOutputValueClass(classOf[BytesWritable]);
            //There is no combiners for now

            jb.setPartitionerClass(classOf[IntermediateFileIndexSortingPartitioner]);
            jb.setOutputKeyComparatorClass(classOf[IntermediateFileIndexSortingComparator]);
            jb.setOutputValueGroupingComparator(classOf[IntermediateFileIndexSortingGroupingComparator]);

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
                    fs.delete(outputDir+"temp", true);
                }
            }
            return ret;
        }

}

class IntermediateFileIndex
    extends Writable
    with Ordered[IntermediateFileIndex] {

    var fn : Text = null;
    var startByte, len : Long = 0;

    def this(fn : String, startByte : Long, len : Long) {
        this();
        this.fn = fn;
        this.startByte = startByte;
        this.len = len
    }
    override def readFields(in : DataInput) {
        if (fn == null) fn = new Text();
        fn.readFields(in);
        startByte = in.readLong;
        len = in.readLong;
    }

    override def write(out : DataOutput) {
        fn.write(out);
        out.writeLong(startByte);
        out.writeLong(len);
    }
    def compare(o : IntermediateFileIndex) : Int = {
        var r : Int = fn.compareTo(o.fn);
        if (r != 0) return r;
        if (startByte > o.startByte) return 1;
        if (startByte < o.startByte) return -1;
        return 0;
    }
}

//partition by filename
class IntermediateFileIndexSortingPartitioner
    extends Partitioner[IntermediateFileIndex, BytesWritable] {
    def getPartition(key : IntermediateFileIndex, value : BytesWritable, numPartitions : Int) : Int = {
        //This is an oddity of Java where hash of string can be negative.
        val l : Long = key.fn.hashCode.longValue - Integer.MIN_VALUE;
        return (l % numPartitions).intValue;
    }
    override def configure(job : JobConf) = {}
}

//order by everything
class IntermediateFileIndexSortingComparator extends RawComparator[IntermediateFileIndex] {
    def compare(b1 : Array[Byte], s1 : Int, l1 : Int, b2 : Array[Byte], s2 : Int, l2 : Int) : Int = {
        val ob1 : ComparableArray[Byte] = b1;
        return ob1.compare(b2);
    }
    def compare(b1 : IntermediateFileIndex, b2 : IntermediateFileIndex) : Int = {
        return b1.compare(b2);
    }
}

//group by file name
class IntermediateFileIndexSortingGroupingComparator
    extends RawComparator[IntermediateFileIndex] {
    val cmpr = new Text.Comparator;
    def compare(b1 : Array[Byte], s1 : Int, l1 : Int,
        b2 : Array[Byte], s2 : Int, l2 : Int) : Int = {
        return cmpr.compare(b1, b2);
    }
    def compare(b1 : IntermediateFileIndex, b2 : IntermediateFileIndex) : Int = {
        return b1.fn.compareTo(b2.fn);
    }
}

//decompressing on the map side introduces intra-cluster transfer inefficiencies but
// allows us to parallel decompression of large files. If it were doen on the reducer
// the compressed block would be sent, but then only one machine would be performing the
// decompression.
class PieceRecreatorMapper(decompressor : (InputStream ⇒ InputStream))
    extends CompressionMRBase
    with Mapper[LongArrayWritable, BytesWritable, IntermediateFileIndex, BytesWritable] {

    lazy val _l = new HZLogger(classOf[PieceRecreatorMapper]);
    var pieceMaker : FilePieceMaker.FilePieceMakerType = null;

    override def configure(conf : JobConf) {
        super.configure(conf);

        pieceMaker =
            FilePieceMaker(conf.getStrings(Common.INTERNAL_JOBCONF_PIECE_SPEC)(0));
    }

    def map(key : LongArrayWritable, value : BytesWritable,
        output : OutputCollector[IntermediateFileIndex, BytesWritable], reporter : Reporter) {

//        _l.error("inside mapper collection id="+key.get(0)+", file part count="+key.get(1)+", the input byte array has size "+value.getBytes.length+", stored size is "+value.getLength);

        val outputBuffer = new ByteArrayOutputStream();
        val inputStream = new ByteArrayInputStream(value.getBytes);

        val decompressedStream = decompressor(inputStream);
        val ab = new Array[Byte](4 * 1024 * 1024); //4mb buffer
        var len = decompressedStream.read(ab);

        //doing the loop this way instead of checking .available, because that doesn't universally work among the third party decompressors
        while (len > 0) {
//            _l.error("PieceRecreator Mapper read in a buffer of length "+len);
            outputBuffer.write(ab, 0, len);
            len = decompressedStream.read(ab);

            //            if (len > 0) {
            //               _l.info("PieceRecreator Mapper read in a buffer of length "+len+"='"+(new String(ab.slice(0, len)))+"'");
            //            }
        }

        val outBytes = outputBuffer.toByteArray;
        val collectionId = key.get(0);
        val filePartCnt = key.get(1);
        val fn : String = pieceMaker.getCollection(collectionId) match {
            case None ⇒
                _l.error("Could not find file names corresponding to collection id"+collectionId);
                return ;
            case Some(x) ⇒
//                _l.error("Found some naem "+x(0).name);
                x(0).name;
        }
//        _l.error("Collected something of length "+outBytes.length);
        val outKey = new IntermediateFileIndex(fn, filePartCnt, outBytes.length);
        output.collect(outKey, outBytes);
    }

}

class PieceRecreatorReducer extends CompressionMRBase
    with Reducer[IntermediateFileIndex, BytesWritable, LongArrayWritable, BytesWritable] {

    private lazy val _l = new HZLogger(classOf[PieceRecreatorReducer]);

    def reduce(key : IntermediateFileIndex, values : Iterator[BytesWritable],
        output : OutputCollector[LongArrayWritable, BytesWritable], reporter : Reporter) {

        val k = key.fn;
//        _l.error("Inside reduce. key is "+k);
        val file = fs.create(outputPath+"/"+k);

        for (byteData ← values) {
            //            _l.info("PieceRecreatorReducer, writing these "+byteData.getBytes.length+" bytes '"+(new String(byteData))+"'.");
//            _l.error("PieceRecreatorReducer, writing these "+byteData.getBytes.length+", the stored length is "+byteData.getLength);
            file.write(byteData.wholeBytesBuffer, 0, byteData.getLength);
        }

        file.close();
    }

}