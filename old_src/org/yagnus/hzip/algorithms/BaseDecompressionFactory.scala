package org.yagnus.hzip.algorithms

import org.apache.hadoop.fs.FileSystem;

/**
 * The Hzip decompressor will 
 * 
 *   a.) take in the name of a compressed archive, an output path, and will unarchive the files into that output path
 *   b.) take in an archive's name, an output path, and a filename glob. The output will be files in the archive matching the glob in the output path.
 * @author u
 *
 */
abstract class BaseDecompressionFactory
    extends HadoopCompressionAlgorithmFactory {

    /**
     * Implementer should generate an algorithm based on the input 
     *  
     * @return
     */
    def generateAlgorithm(args : Array[String], inputArchiveFilename : String, outputPath : String) : Option[HadoopCompressionAlgorithm];
    var fs:FileSystem=null;
    def parseCommandline(fs : FileSystem, args : Array[String]) : Option[HadoopCompressionAlgorithm] = {
        if (!parametersAreAplicable(args)) {
            return None;
        }

        this.fs = fs;
        if(args.length<3){
            println("Could not parse the input file or the output path.");
            return None;
        }
        val inputArchiveName = args(1);
        val outputDir = args(2);

        return generateAlgorithm(args, inputArchiveName, outputDir);
    }

}