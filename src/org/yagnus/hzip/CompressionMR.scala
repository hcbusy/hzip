package org.yagnus.hzip
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.JobConf
import org.yagnus.hzip.chunk.Chunk
import org.yagnus.hzip.chunk.Chunks
import org.apache.hadoop.fs.Path
import org.apache.hadoop.filecache.DistributedCache
import org.yagnus.scalasupport.DelayedInitImpl
import org.yagnus.hzip.io.HZVer
import org.yagnus.hzip.chunk.Chunker
import org.yagnus.hzip.chunk.ChunkIOs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory
import org.yagnus.yadoop.HdfsUtils
import java.net.URI
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.File
import scala.reflect.BeanProperty
import scala.io.Source

/**
 * This is the base class for all compression related Map-Reduce
 *
 * MRB = "Map Reduce Base"
 *
 */
trait CompressionMR[Kin, Vin, Kout, Vout] extends CompressionMRBase[Kin, Vin, Kout, Vout] with HZVer with HZLogger {

    private lazy final implicit val LOG_AS = initLogger(classOf[CompressionMR[Kin, Vin, Kout, Vout]]);

    addVersion(1);

    @BeanProperty
    var chunks: Chunks = _;

    def readChunks(fp: Path): Chunks = new ChunksIO().deserializeFromBytes(HdfsUtils.readFully(fs, fp));
    def readChunks(fp: String): Chunks = {
        val f = new File(fp);
        val fi = new FileInputStream(f);
        val thechunks = new Array[Byte](f.length().intValue);
        fi.read(thechunks);
        return new ChunksIO().deserializeFromBytes(thechunks);
    }
    override def configure(jb: Configuration) = {
        super.configure(jb);
        //    val tpath = caches.filter(_.getName().contains(RuntimeConstants.CHUNK_FILE_NAME));
        var ret: Chunks = null;
        try {
            if (caches != null) {
                val tpath = caches.filter(_.getName.contains(RuntimeConstants.CHUNK_FILE_NAME));
                if (tpath != null && tpath.size > 0) {
                    info("Will try to read chunk from '" + tpath(0).toUri.getPath + "'. " + tpath(0).toUri());
                    ret = readChunks(tpath(0));
                }
            }
        } catch {
            case x: Throwable =>
                ret = null;
                info(x, "Cache retrieve of ChunksIO failed, going to read from global source");
        }

        if (ret == null) {
            //deserialize from original configuration file
            val confs = job.getStrings(RuntimeConstants.conf.CHUNK_DATA);
            if (confs != null && confs.size > 0) {
                info("retrieving chunk data from:" + confs(0));
                val cfp = new Path(confs(0));
                ret = readChunks(cfp);
            }
        }
        chunks = ret;
    }

}

object CompressionMRUtil extends CompressionMR[Unit, Unit, Unit, Unit];