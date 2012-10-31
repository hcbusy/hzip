package org.yagnus.hzip.thirdParty.algorithms

import org.yagnus.hzip.algorithms._;
import org.yagnus.hzip.Common;
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import scala.collection.immutable.List;

class ThirdPartyDecompressionFactory extends BaseDecompressionFactory {
    def generateAlgorithm(args : Array[String], inputArchiveFilename : String, outputPath : String) : Option[HadoopCompressionAlgorithm] =
        args(0).toLowerCase match {
            case "gunzip" ⇒
                Some(new GzipDecompression(fs, List(new Path(inputArchiveFilename)), List(outputPath), Common.GLOBAL_TEMP))
            case "bunzip"  ⇒
                Some(new Bzip2Decompression(fs, List(new Path(inputArchiveFilename)), List(outputPath), Common.GLOBAL_TEMP))
            case "unzip" ⇒
                Some(new ZipDecompression(fs, List(new Path(inputArchiveFilename)), List(outputPath), Common.GLOBAL_TEMP))
            case _ ⇒ None;
        }

    def parametersAreAplicable(args : Array[String]) = args.length > 0 && (args(0).toLowerCase match {
        case "gunzip" | "bunzip" | "unzip" ⇒
            true;
        case _ ⇒ false;
    })
}