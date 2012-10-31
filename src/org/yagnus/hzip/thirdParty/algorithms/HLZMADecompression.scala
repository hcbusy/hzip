package org.yagnus.hzip.thirdParty.algorithms

import java.io.InputStream;
import net.contrapunctus.lzma.LzmaInputStream;
import org.yagnus.hzip.pieces.{ PieceRecreatorMapper, PieceRecreator };
import org.apache.hadoop.fs.{ FileSystem, PathFilter, Path };

class HLZMADecompression(fs : FileSystem, theSource : String, theOutputDir : String, pathFilter : PathFilter)
    extends PieceRecreator(fs, theSource, theOutputDir, pathFilter) {

    def this(fs : FileSystem, theSource : String, theOutputDir : String) {
        this(fs, theSource, theOutputDir, new PathFilter { override def accept(p : Path) : Boolean = true });
    }

    override def mapperClass = classOf[HLZMADecompressionMapper];

}

class HLZMADecompressionMapper extends PieceRecreatorMapper((is : InputStream) ⇒ new LzmaInputStream(is));