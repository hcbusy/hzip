package org.yagnus.hzip.chunk.impl;

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import org.slf4j.LoggerFactory
import org.yagnus.hzip.CompressionMR
import org.yagnus.scalasupport.iterates._
import org.yagnus.hzip.chunk.SubChunk
import org.yagnus.hzip.chunk.Chunk
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.fs.Path
import org.yagnus.hzip.chunk.ChunkIOs._
import org.yagnus.yadoop.Yadoop._
import org.yagnus.scalasupport.iterates.IterateJavaCollections._
import java.io.File
import org.apache.hadoop.filecache.DistributedCache
import org.yagnus.hzip.BWT.BWTCompressionConstants
import org.apache.hadoop.conf.Configuration

/**
 * The FileChunker produces chunks that are files, so the task here is to simply write them back
 * to the disk
 */
class FileChunkRefileReducer(processor: Array[Byte] => Array[Byte])
    extends Reducer[LongWritable, BytesWritable, LongWritable, IntWritable]
    with CompressionMR[LongWritable, BytesWritable, LongWritable, IntWritable] {
    private lazy val logger = LoggerFactory.getLogger(classOf[FileChunkRefileReducer]);

    def this() {
        this(x => x)
    }

    override protected def setup(context: ReducerContext) {
        super.setup(context);
        super.configure(context.getConfiguration());
    }

    override protected def reduce(chunkId: LongWritable, values: java.lang.Iterable[BytesWritable],
        context: ReducerContext) {

        val chunk = chunks(chunkId);
        val fn = chunk.getSubChunks()(0).getFn();
        val ofn = outputPath + "/" + fn;
        logger.info("writing '" + ofn + "' for chunk " + chunk.getChunkId);

        val ofile = fs.create(ofn);
        if (ofile == null) {
            logger.error("Could not output to file '" + ofn + "' during refiling operation, this file will not be written to.");
            context.write(chunk.getChunkId, -1);
        } else {
            var progress = 0l;
            //assert(values.size==1);

            for (scd <- values.iterator) {
                val obts = processor(scd.getBytes)

                ofile.write(obts, 0, scd.getLength);
                progress += obts.size;
                logger.info("Wrote " + obts.size + " to " + ofn);
                context.progress;
                ofile.flush();
            }

            ofile.close;
            context.write(chunk.getChunkId, progress.intValue);
        }
    }

}
