package org.yagnus.hzip.chunk.impl;
import scala.reflect.BeanProperty
import org.yagnus.hzip.AppConstants
import org.yagnus.hzip.io.HZIO
import org.yagnus.hzip.CompressionAlgorithmFactory
import org.yagnus.hzip.RuntimeConstants
import org.apache.hadoop.filecache.DistributedCache
import org.yagnus.hzip.CompressionAlgorithmStep
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.FileSystem
import org.yagnus.hzip.chunk.ChunkIOs._
import org.yagnus.yadoop.Yadoop._
import org.yagnus.yadoop.HdfsUtils
import org.yagnus.hzip.chunk._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import scala.collection.mutable.HashSet
import org.yagnus.hzip.HZLogger
import java.util.concurrent.atomic.AtomicLong

/**
  *
  * The hzip compresses directories. So fn's will always be relative to the root output directory
  *
  */

class FileChunker extends Chunker with HZLogger {
    private implicit lazy val LOG_AS = initLogger(classOf[FileChunker]);
    addVersion("0.0.1");
    val nextId = new AtomicLong(0)
    @BeanProperty
    var chunkLocation: String = _;

    def add(fn: String, len: Long) = {

        val id: Long = nextId.getAndIncrement;
        val chnk = new Chunk(id);
        val sc = new SubChunk(id, 0, fn, 0, len);
        sc.addVersion(getVersion);
        chnk.addSubChunk(sc);
        chnk.setLength(len);

        myChunks.add(chnk);
        chnk
    }

    protected def writeChunkFile(p: Path) {
        val pf = fs.create(p);
        val chunkData = myChunks.serializeToBytes;
        import scala.math.min;
        debug("Goina write this chunk:" + new String(chunkData).substring(0, min(chunkData.length, 256)) + "... (size " + chunkData.length + ") to " + p.toUri().getPath);
        pf.write(chunkData);
        pf.flush;
        pf.close;
    }

    override def getCompressionAlgorithm: Seq[CompressionAlgorithmStep] = {

        chunkLocation = output + AppConstants.CHUNK_FILE_NAME;
        info("File Chunker writing to " + chunkLocation);
        val chunkFilePath = new Path(chunkLocation);

        Seq(new CompressionAlgorithmStep() {
            override def preHadoop {
                val mkdr = fs.mkdirs(new Path(output));

                //Scan input directory for files.
                val (fls, dns) = HdfsUtils.getFileDirStatus(fs, Seq(input), null, true);
                val seen = new HashSet[String]();
                for (fl <- fls) {
                    val flnm = fl.getPath().toUri().getPath();
                    if (!seen.contains(flnm)) {
                        val flln = fl.getLen;
                        info("Adding :" + flnm + " which has length " + flln);
                        add(flnm, flln);
                        seen.add(flnm);
                    }
                }
                writeChunkFile(chunkFilePath);
            }

            override def postHadoop {
                HdfsUtils.cleanMROutputDir(fs, output);
            }

            override def getHadoop = {
                conf.setStrings(RuntimeConstants.conf.CHUNK_DATA, chunkLocation);
                conf.setStrings(RuntimeConstants.conf.OUTPUT_PATH, output);
                conf.setStrings(RuntimeConstants.conf.TEMP_DIR, temp);
                DistributedCache.addCacheFile(chunkFilePath, conf);

                val jb = new Job(conf, "FileChunker chunk");
                FileInputFormat.setInputPaths(jb, myChunks.getFileNames.map(new Path(_)).toArray[Path]: _*)
                FileOutputFormat.setOutputPath(jb, new Path(output + RuntimeConstants.CHUNK_PATH));

                jb.setMapperClass(classOf[ChunkMapper]);
                jb.setReducerClass(classOf[ChunkReducer]);

                jb.setInputFormatClass(classOf[ChunkInputFormat]);
                jb.setOutputFormatClass(classOf[SequenceFileOutputFormat[LongWritable, BytesWritable]]);

                jb.setMapOutputKeyClass(classOf[SubChunkIO]);
                jb.setMapOutputValueClass(classOf[BytesWritable]);

                jb.setOutputKeyClass(classOf[LongWritable]);
                jb.setOutputValueClass(classOf[BytesWritable]);

                jb.setPartitionerClass(classOf[SubChunkPartitioner]);

                jb.setGroupingComparatorClass(classOf[SubChunkGrouper]);
                jb.setSortComparatorClass(classOf[SubChunkSorter]);

                //make sure chunks are written to separate files.
                jb.setNumReduceTasks(myChunks.getChunks.size + 2);

                jb.setJarByClass(classOf[ChunkMapper]);

                jb;
            }
        })
    };

    override def getDecompressionAlgorithm: Seq[CompressionAlgorithmStep] = {
        directorifyInput;

        chunkLocation = input + AppConstants.CHUNK_FILE_NAME;
        val chunkFilePath = new Path(chunkLocation);
        return Seq(new CompressionAlgorithmStep() {

            override def postHadoop {
                HdfsUtils.cleanMROutputDir(fs, output);
            }

            override def getHadoop = {

                conf.setStrings(RuntimeConstants.conf.CHUNK_DATA, chunkLocation);
                conf.setStrings(RuntimeConstants.conf.OUTPUT_PATH, output);
                conf.setStrings(RuntimeConstants.conf.TEMP_DIR, temp);
                DistributedCache.addCacheFile(chunkFilePath, conf);

                val jb = new Job(conf, "FileChunker dechunk");

                val (previousReduceOut, _) = HdfsUtils.getFileDirStatus(fs, input + RuntimeConstants.CHUNK_PATH, ".*/part-r-[0-9]*$", true);
                info("looking for files in " + input + RuntimeConstants.CHUNK_PATH + ":" + previousReduceOut.size);
                FileInputFormat.setInputPaths(jb, previousReduceOut.map(_.getPath): _*);
                info("The job will have input:" + jb.getConfiguration().getStrings("mapred.input.dir").reduce((a, b) => a + "," + b));

                FileOutputFormat.setOutputPath(jb, temp + "/FileDechunkingResults");

                jb.setMapperClass(classOf[Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]]);
                jb.setReducerClass(classOf[FileChunkRefileReducer]);

                jb.setInputFormatClass(classOf[SequenceFileInputFormat[LongWritable, BytesWritable]]);
                jb.setOutputFormatClass(classOf[SequenceFileOutputFormat[LongWritable, IntWritable]]);

                jb.setMapOutputKeyClass(classOf[LongWritable]);
                jb.setMapOutputValueClass(classOf[BytesWritable]);

                jb.setOutputKeyClass(classOf[LongWritable]);
                jb.setOutputValueClass(classOf[IntWritable]);

                //                jb.setPartitionerClass(classOf[ChunkPartitioner]);
                //                jb.setGroupingComparatorClass(classOf[ChunkSorter]);
                //                jb.setSortComparatorClass(classOf[ChunkSorter]);

                jb.setJarByClass(classOf[FileChunkRefileReducer]);

                jb;
            }
        })
    };

    override def getDisplayName = "fChunk";
    override def parseCommandline(params: Seq[String]): Option[CompressionAlgorithmFactory] = {
        if (op != null && op == "fChunk") {
            val inputPath = new Path(input);
            if (input == null || input.length() == 0) {
                debug("FileChunker wasn't configured");
                return None;
            } else {
                directorifyOutput;
                debug("Configured FileChunker.");
                return Some(this);
            }
        } else None;
    }

    override def skipHzStamp = true;
}
