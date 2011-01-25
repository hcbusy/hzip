package org.yagnus.hzip.algorithms

import org.apache.hadoop.fs.FileSystem;

class HzipCompression(fs : FileSystem, args : Seq[String]) extends HadoopCompressionAlgorithm(fs) {
    def createCompressionJob() : CompressionJob = new CompressionJob(null){
    }
}