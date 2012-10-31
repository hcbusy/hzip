package org.yagnus.hzip.thirdParty.algorithms;

import java.io.InputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.yagnus.hzip.pieces.{ PieceRecreatorMapper, PieceRecreator };
import org.apache.hadoop.fs.{ FileSystem, PathFilter, Path };

class HBzip2Decompression(fs : FileSystem, theSource : String, theOutputDir : String, pathFilter : PathFilter)
    extends PieceRecreator(fs, theSource, theOutputDir, pathFilter) {
    
    def this(fs : FileSystem, theSource : String, theOutputDir : String){
        this(fs,theSource,theOutputDir, new PathFilter { override def accept(p : Path) : Boolean = true });
    }

    override def mapperClass = classOf[HBzip2DecompressionMapper];
}

class HBzip2DecompressionMapper extends PieceRecreatorMapper((is : InputStream) ⇒ new BZip2CompressorInputStream(is));