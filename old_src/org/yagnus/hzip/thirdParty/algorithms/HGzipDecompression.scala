package org.yagnus.hzip.thirdParty.algorithms

import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.yagnus.hzip.pieces.{ PieceRecreatorMapper, PieceRecreator };
import org.apache.hadoop.fs.{ FileSystem, PathFilter,Path };

class HGzipDecompression(fs : FileSystem, theSource : String, theOutputDir : String, pathFilter : PathFilter)
    extends PieceRecreator(fs, theSource, theOutputDir, pathFilter) {

    def this(fs : FileSystem, theSource : String, theOutputDir : String) {
        this(fs, theSource, theOutputDir, new PathFilter { override def accept(p : Path) : Boolean = true });
    }

    override def mapperClass = classOf[HGzipDecompressionMapper];

}

class HGzipDecompressionMapper extends PieceRecreatorMapper((is : InputStream) ⇒ new GZIPInputStream(is));