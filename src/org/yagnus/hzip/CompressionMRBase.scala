package org.yagnus.hzip
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache
import org.yagnus.hzip.chunk.Chunks
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

trait CompressionMRBase[Kin, Vin, Kout, Vout] extends HZLogger {
    private implicit final lazy val LOG_AS = initLogger(classOf[CompressionMRBase[Kin, Vin, Kout, Vout]]);

    type MapperContext = Mapper[Kin, Vin, Kout, Vout]#Context;
    type ReducerContext = Reducer[Kin, Vin, Kout, Vout]#Context;

    var job: Configuration = _;
    var fs: FileSystem = _;
    var tempDir: String = null;
    var outputPath: String = null;
    var caches: Array[org.apache.hadoop.fs.Path] = _;

    def configure(jb: Configuration) = {
        info("Configure was called.");
        job = jb;
        fs = FileSystem.get(job);
        tempDir = job.getStrings(RuntimeConstants.conf.TEMP_DIR)(0);
        outputPath = job.getStrings(RuntimeConstants.conf.OUTPUT_PATH)(0);
        caches = DistributedCache.getLocalCacheFiles(job);
    }
}