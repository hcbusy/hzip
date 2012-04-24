package org.yagnus.attic

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yagnus.yadoop.Yadoop._;

//perform bytewise comparison
//returns 1 if different, 0 if same.
object Cmp {

    val bufferSize = (1024 * 4 * 1024); //1mpages, 4mb;;
    def diff(fs : FileSystem, f1 : String, f2 : String) : Int = {
        val block1 = new Array[Byte](bufferSize); //1mpages, 4mb;
        val block2 = new Array[Byte](bufferSize); //1mpages, 4mb;
        var blockSize1 = 0;
        var blockSize2 = 0;

        //brilliant optimisation!
        if (f1 == f2) {
            return 0;
        }

        val s1 = fs.getFileStatus(f1);
        val s2 = fs.getFileStatus(f2);

        if (s1.getLen != s2.getLen) {
            return 1;
        }

        val i1 = fs.open(s1.getPath);
        val i2 = fs.open(s2.getPath);

        do {
            blockSize1 = i1.read(block1);
            blockSize2 = i2.read(block2);

            if (blockSize1 != blockSize2) {
                return 1; //bad
            }

            for (i ← 0 until blockSize1) {
                if (block1(i) != block2(i))
                    return 1;
            }

        } while (blockSize1 > 0);

        return 0;
    }
}