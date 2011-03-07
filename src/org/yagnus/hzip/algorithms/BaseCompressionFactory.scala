package org.yagnus.hzip.algorithms

import org.apache.hadoop.fs._;
import org.yagnus.hzip.HdfsUtils;
import org.yagnus.hzip.pieces.{ PieceMaker, FilePieceMaker };

/**
 * The Hzip compression archiver will always take in a list or pattern of file names
 * and output one file that contains all the other files, compressed.
 * 
 * 
 * @author hsb
 * 
 *
 */
abstract class BaseCompressionFactory
    extends HadoopCompressionAlgorithmFactory {

    /**
     * Implementer should generate an algorithm based on the input 
     *  
     * @return
     */
    def generateAlgorithm(args : Array[String], outputArchiveFilename : String, inputFiles : Seq[FileStatus], emptyDirectories : Seq[FileStatus], pm:FilePieceMaker) : Option[HadoopCompressionAlgorithm];

    /**
     * Deriver can define a factory method to construct a pieceMaker for this compression job
     * 
     */
    def makePieceMaker : FilePieceMaker = new FilePieceMaker;

    var fs : FileSystem = null; ;
    def parseCommandline(fs : FileSystem, args : Array[String]) : Option[HadoopCompressionAlgorithm] = {
        if (!parametersAreAplicable(args)) {
            return None;
        }
        if (args.length < 3) {
            println("Not enough parameters.");
            return None;
        }

        val archiveName = args(1);
        this.fs = fs;
        val pm = makePieceMaker;
        val (filePaths, dirPaths) = HdfsUtils.getFileDirStatus(fs, args.slice(2, args.length),
            new PathFilter { override def accept(p : Path) : Boolean = true },
            true, pm);
        return generateAlgorithm(args, archiveName, filePaths, dirPaths, pm);
    }
}