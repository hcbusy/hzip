package org.yagnus.hzip;

import org.yagnus.hzip.algorithms._;
import org.apache.hadoop.fs.FileSystem
import org.yagnus.yadoop.Cmp;

class CmpFactory extends HadoopCompressionAlgorithmFactory {
    def parseCommandline(fs : FileSystem, args : Array[String]) : Option[HadoopCompressionAlgorithm] = {
        val f1 = args(1); val f2 = args(2);
        return Option(new HadoopCompressionAlgorithm(fs) {
            def createCompressionJob() = new CompressionJob(null) {
                var retCode = 0;
                override def preHadoop() = {
                    retCode = Cmp.diff(fs, f1, f2);
                }
                override def exitCode = retCode;

            }
        });
    }
    def parametersAreAplicable(args : Array[String]) = (args.length > 1 && args(0).toLowerCase == "cmp")

}
