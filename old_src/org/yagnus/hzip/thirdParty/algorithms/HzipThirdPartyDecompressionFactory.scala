package org.yagnus.hzip.thirdParty.algorithms

import org.yagnus.hzip.algorithms._;

import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._

class HzipThirdPartyDecompressionFactory extends BaseDecompressionFactory {

    def generateAlgorithm(args : Array[String], inputArchiveFilename : String, outputPath : String) : Option[HadoopCompressionAlgorithm] =
        args(0).toLowerCase match {
            case "hgunzip" ⇒ 
                Some(new HGzipDecompression(fs, inputArchiveFilename, outputPath, new PathFilter { override def accept(p : Path) : Boolean = true }))
            case "hbunzip" ⇒
                Some(new HBzip2Decompression(fs, inputArchiveFilename, outputPath, new PathFilter { override def accept(p : Path) : Boolean = true }))
            case _ ⇒ None;
        }

    def parametersAreAplicable(args : Array[String]) = args.length > 0 && (args(0).toLowerCase match {
        case "hgunzip" | "hbunzip" ⇒
            true;
        case _ ⇒ false;
    })
}