package org.yagnus.hzip.thirdParty.algorithms;

import org.yagnus.hzip.{HZLogger, Common};
import org.yagnus.hzip.algorithms.CompressionJob;
import org.yagnus.yadoop.Yadoop._;
import org.yagnus.hzip.algorithms._;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.conf.Configuration;

import scala.collection.mutable.ArrayBuffer;
import scala.util.Random;

import java.io.InputStream;
import java.util.zip.{ ZipEntry, ZipInputStream };

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
/**
 * 	Decompresses the "zip" archives
 * 
 * @author u
 *
 */
class ZipDecompression(fs : FileSystem, gFiles : Seq[Path], oFile : Seq[String], globalTempDir : String)
        extends FileNameDecompressionAlgorithm(fs, gFiles, oFile, globalTempDir) {

    def mapperClass : java.lang.Class[_ <: Mapper[_, _, _, _]] = classOf[ZipDecompressionMapper];
}

class ZipDecompressionMapper extends FileNameDecompressionMapper(null) {
    private lazy val _l = new HZLogger(classOf[ZipDecompressionMapper]);

    val decompress = (inputStream : InputStream) ⇒ new ZipInputStream(inputStream);

    //makes sure the directory exists
    def ensureFilePath(fn : String) {
        val tind = fn.lastIndexOf("/");
        val dirname = fn.slice(0, tind);
        fs.mkdirs(dirname);
    }
    override def map(key : Text, value : Text,
        output : OutputCollector[IntWritable, IntWritable],
        reporter : Reporter) = {

        val inFn : String = key;
        _l.info("Decompressing "+inFn);
        val inZipFile = getInputStream(inFn);
        val inFile = decompress(inZipFile);

        val outDir : String = value;
        var entry : ZipEntry = null;
        do { //loop through all the ZipEntries

            entry = inFile.getNextEntry;
            if (entry != null) {

                var ofn = entry.getName;
                _l.info("Decompressing "+ofn+" into "+outDir);
                if (ofn.startsWith("/")) {
                    ofn = outDir + ofn
                } else {
                    ofn = outDir + '/' + ofn;
                }

                if (entry.isDirectory) {

                    fs.mkdirs(ofn); //recursive directory creation

                } else {
                    //It's a file. Let's write the data out.
                    ensureFilePath(ofn);
                    val outFile = fs.create(ofn);

                    val buffer = new Array[Byte](fs.getDefaultBlockSize.intValue);

                    var bytesRead = inFile.read(buffer);
                    do {
                        outFile.write(buffer, 0, bytesRead);
                        bytesRead = inFile.read(buffer);
                    } while (bytesRead > 0);

                    outFile.close();
                }
                //map does not yeild any results, as there is no reduction to be performed.
            }
        } while (entry != null);
    }
}