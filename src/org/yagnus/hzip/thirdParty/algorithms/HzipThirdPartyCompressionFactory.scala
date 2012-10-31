package org.yagnus.hzip.thirdParty.algorithms;

import org.apache.hadoop.fs.{ FileStatus, FileSystem };
import org.yagnus.hzip.algorithms._;
import org.yagnus.hzip.pieces._;

class HzipThirdPartyCompressionFactory extends BaseCompressionFactory {

    def generateAlgorithm(args : Array[String], outputArchiveFilename : String, inputFiles : Seq[FileStatus], emptyDirectories : Seq[FileStatus], pieceMaker : FilePieceMaker) : Option[HadoopCompressionAlgorithm] =
        args(0).toLowerCase match {
            case "hgzip" | "hgz" ⇒
                Some(new HGzipCompression(fs, inputFiles, emptyDirectories, outputArchiveFilename, pieceMaker))
            case "hbzip" | "hbzip2" | "hbz" | "hbz2" ⇒
                Some(new HBzip2Compression(fs, inputFiles, emptyDirectories, outputArchiveFilename, pieceMaker))
            case "lzma" =>
                Some(new HLZMACompression(fs, inputFiles, emptyDirectories, outputArchiveFilename, pieceMaker))
            case _ ⇒ None;
        }

    def parametersAreAplicable(args : Array[String]) = args.length > 1 && (args(0).toLowerCase match {
        case "hgzip" | "hgz" | "hbzip" | "hbzip2" | "hbz" | "hbz2"|"lzma" ⇒
            true;
        case _ ⇒ false;
    })

}