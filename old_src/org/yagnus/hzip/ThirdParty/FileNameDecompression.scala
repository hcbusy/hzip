package org.yagnus.hzip.ThirdParty

import org.yagnus.hzip.{ HadoopCompressionAlgorithm, CompressionJob, Common };
import org.yagnus.yadoop._;
import org.yagnus.yadoop.Yadoop._;

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred._;

import scala.collection.mutable.ArrayBuffer;
import scala.util.Random;

import java.io.InputStream;

abstract class FileNameDecompressionAlgorithm(fs : FileSystem, gFiles : Seq[Path], oFile : Seq[String], globalTempDir : String)
        extends HadoopCompressionAlgorithm(fs) {

    var tfn : Array[String] = null;
    val manualSplits = 5; //TODO: make this configurable
    val myTempDir = globalTempDir+"/decompression_"+Random.nextLong;

    def createCompressionJob() : CompressionJob = {

        var sw = new Array[SequenceFile.Writer](manualSplits);

        println("decompressing these files:")

        // setup job
        val jb = new JobConf();
        jb.setInputFormat(classOf[SequenceFileInputFormat[Text, Text]]);
        jb.setOutputFormat(classOf[SequenceFileOutputFormat[IntWritable, IntWritable]]);

        jb.setCompressMapOutput(false);

        jb.setMapOutputKeyClass(classOf[IntWritable]);
        jb.setMapOutputValueClass(classOf[IntArrayWritable]);

        jb.setOutputKeyClass(classOf[IntWritable]);
        jb.setOutputValueClass(classOf[BytesWritable]);

        jb.setMapperClass(mapperClass);
        jb.setReducerClass(classOf[IdentityReducer[IntWritable, IntWritable]]);

        //NOTE: Ideally, manualSplit is created so that each machine can only hold one map, this way running that many
        // maps maximizes parallelism.
        jb.setNumMapTasks(manualSplits);
        //
        jb.setNumReduceTasks(1); // restricted to 1 reducer.
        jb.setNumTasksToExecutePerJvm(1); // too memory intensive
        jb.setJarByClass(classOf[FileNameDecompressionMapper]);

        tfn = new Array[String](manualSplits);
        for (ind ← 0 until manualSplits) {
            tfn(ind) = myTempDir+"/"+ind+"_"+(new Random()).nextLong();
            FileInputFormat.addInputPath(jb, tfn(ind));

        }

        val MROutputPath = myTempDir+"/finalOutput";
        FileOutputFormat.setOutputPath(jb, MROutputPath);

        val ret = new CompressionJob(jb) {
            override def preHadoop() {
                for (offset ← 0 until manualSplits) {
                    var ind = offset % manualSplits;
                    sw(ind) = SequenceFile.createWriter(fs, jb,
                        new Path(tfn(ind)), classOf[Text], classOf[Text]);
                    jb.setOutputFormat(classOf[SequenceFileOutputFormat[Text, Text]]);
                }

                //write the files
                for (ind ← 0 until gFiles.length)
                    sw(ind % manualSplits).append(__s2t(gFiles(ind).toUri.getPath.toString), __s2t(oFile(ind)));

                //and then close them
                for (swf ← sw) swf.close();
            }

            override def postHadoop() = {
                println("Deleting temp files");

            }

        }
        return ret;
    }

    def mapperClass : java.lang.Class[_ <: Mapper[_, _, _, _]];
}

class FileNameDecompressionMapper(decompress : InputStream ⇒ InputStream)
        extends MapReduceBase
        with Mapper[Text, Text, IntWritable, IntWritable] {

    import org.apache.hadoop.conf.Configuration;
    import java.io.BufferedInputStream;
    import java.util.zip.GZIPInputStream;

    val READ_BUFFER_BLOCK_SIZE = 1;
    def map(key : Text, value : Text,
        output : OutputCollector[IntWritable, IntWritable],
        reporter : Reporter) = {

        val inFn : String = key;
        val outFn : String = value;

        val fs = FileSystem.get(new Configuration());

        val readBlockSize = READ_BUFFER_BLOCK_SIZE * fs.getBlockSize(inFn).intValue;

        val inZipFile = new BufferedInputStream(fs.open(inFn), readBlockSize);
        val outFile = fs.create(outFn);

        val inFile = decompress(inZipFile);
        val buffer = new Array[Byte](fs.getDefaultBlockSize.intValue);
        var bytesRead = inFile.read(buffer);
        do {
            outFile.write(buffer, 0, bytesRead);
            bytesRead = inFile.read(buffer);
        } while (bytesRead > 0);
        //map does not yeild any results, as there is no reduction to be performed.
    }
}