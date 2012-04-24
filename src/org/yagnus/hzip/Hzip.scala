package org.yagnus.hzip;
import scala.math.abs
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.slf4j.LoggerFactory
import org.yagnus.scalasupport.YReflections
import org.yagnus.yadoop.Yadoop._

object Hzip extends App {
    println("Welcome to HZip, high quality compression for hadoop.(version " + AppConstants.LATEST_HZIP_APP_VERSION + ")");

    var debugLevel = 0;

    val rnd = new Random();

    val fs: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new Configuration(true));

    var tmpDirName: String = null;
    var tmpDir: Path = null;
    //    val hadoopOpts = new GenericOptionsParser(args);

    /****Check mandatory inputs****/
    if (args.length < 3) error("Please enter the operation, input and output.");
    val op = args(0);
    var input = args(1);
    var output = args(2);
    val spArgs: IndexedSeq[String] = if (args.length > 3) args.slice(3, args.length) else IndexedSeq();

    //  val istat = ;
    if (!fs.isFile(input) && !fs.getFileStatus(input).isDir) {
        error("Input file " + input + " didn't exist.");
    }
    if (!output.endsWith("/")) output += "/";

    val decompression = input.endsWith(".hz") || fs.isFile(input + "/" + AppConstants.HZIP_ARCHIVE_MARKER);

    val conf = new Configuration();
    conf.setStrings(RuntimeConstants.conf.TEMP_DIR, tmpDirName);
    conf.setInt(RuntimeConstants.conf.DEBUG_LEVEL, debugLevel);

    //to do set the logging level

    do {
        tmpDirName = AppConstants.TEMP_FILE_DIR_ROOT + abs(rnd.nextLong)
        tmpDir = new Path(tmpDirName);
    } while (!fs.mkdirs(tmpDir));

    tmpDirName += "/";
    //    debug("The tempdir is at " + tmpDirName);

    try {

        //get available algorithms
        val algos = YReflections.getClasses("org.yagnus.hzip", "org\\.yagnus\\.hzip.*", classOf[CompressionAlgorithmFactory]);
        println("Scanning found " + algos.size + " classes to check ");
        //check to see if we have configurations for them.
        val specdAlgo = algos.map((x: Class[CompressionAlgorithmFactory]) => {
            val f = x.newInstance();
            f.init(op, input, output, tmpDirName, conf, fs);
            println("Checking " + f);
            f.parseCommandline(spArgs);
        }); //.flatten; hmm couldn't do this in 2.9.1

        for (af <- specdAlgo.flatten) {

            val curAlgo = if (decompression) {
                println("Decompressing " + af.getDisplayName);
                af.getDecompressionAlgorithm;
            } else {
                println("Compressing " + af.getDisplayName);
                af.getCompressionAlgorithm;
            }
            println("There are " + curAlgo.size + " hadoop jobs.");
            for (cas <- curAlgo) {
                cas.preHadoop;
                val hjb = cas.getHadoop
                if (hjb != null) {
                    //JobClient.runJob(hconf);
                    val result = hjb.waitForCompletion(true);
                    if (!result) {
                        println("Running job resulted in error. exiting.");
                    }

                }

                if (!af.skipHzStamp) {
                    hzStamp;
                    println("\tFinishing up " + cas.getDisplayName);
                }

                cas.postHadoop;
            }
        }
        println("done.");
    } finally {
        if (debugLevel > 0) {
        } else {
            fs.delete(tmpDir, true);
        }
    }
    def error(input: String*) {
        println(input.reduceLeft((l, r) => l + r));
        exit(-1);
    }

    def hzStamp() {
        val mrkr = fs.create(output + AppConstants.HZIP_ARCHIVE_MARKER)
        mrkr.write(0);
        mrkr.close;

    }
}