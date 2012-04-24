package org.yagnus.hzip.chunk.impl;

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import org.slf4j.LoggerFactory
import org.yagnus.hzip.CompressionMR
import org.yagnus.scalasupport.iterates._
import org.yagnus.hzip.chunk.SubChunk
import org.yagnus.hzip.chunk.Chunk
import org.yagnus.hzip.chunk.ChunkIOs._
import org.yagnus.yadoop.Yadoop._
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.BytesWritable
import org.yagnus.scalasupport.iterates.IterateJavaCollections._
import org.apache.hadoop.io.LongWritable

class ChunkReducer(compressTo: OutputStream ⇒ OutputStream)
    extends Reducer[SubChunkIO, BytesWritable, LongWritable, BytesWritable]
    with CompressionMR[SubChunkIO, BytesWritable, LongWritable, BytesWritable] {

    private lazy val logger = LoggerFactory.getLogger(classOf[ChunkReducer]);

    setVersion("1");

    //By default we don't compress the output
    def this() {
        this((x: OutputStream) ⇒ x);
    }

    override protected def setup(context: ReducerContext) {
        super.setup(context);
        super.configure(context.getConfiguration());
    }

    override protected def reduce(key: SubChunkIO, values: java.lang.Iterable[BytesWritable], context: ReducerContext) {

        val chunk = chunks.getChunk(key.getChunkId());

        val ow = new ByteArrayOutputStream(chunk.getLength.intValue);
        val cw = compressTo(ow);

        var progress = 0l;
        logger.info("Inside chunk reducer for chunk:" + chunk.getChunkId + ", chunk has size:" + chunk.getLength);
        for (value ← values.iterator) {
            val key = context.getCurrentKey();
            logger.info("\t   subsubchunk:" + key + " the offset progress before processing is :" + progress);
            assert(key.getStart == progress);
            cw.write(value, 0, key.getLength.intValue);
            progress += value.getLength;
            context.progress;
        }

        cw.close();
        ow.close();

        context.write(chunk.getChunkId, ow.toByteArray);
    }
}