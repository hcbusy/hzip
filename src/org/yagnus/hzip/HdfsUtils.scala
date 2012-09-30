package org.yagnus.hzip

import org.yagnus.hzip.pieces._;
import org.yagnus.hzip.Common._;

import org.yagnus.yadoop.LongArrayWritable;
import org.yagnus.yadoop.Yadoop._;

import java.io.{ DataInput, DataOutput }
import java.util.Iterator
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import scala.collection.mutable.{ HashMap, ArrayBuffer };
import scala.xml._;

object HdfsUtils {
    /**
     * This is documentation for the following method, which shall split the data into compressible blocks which are known as baches in this program. This
     * code will have documentation even if eclipse refuses to receive them by crashing when I try to document it.
     * 
     * 
     */
    def getFileDirStatus(fs : FileSystem, inFiles : Seq[String], pathFilter : PathFilter, recursive : Boolean, pieceMaker : PieceMaker[String]) : Tuple2[List[FileStatus], List[FileStatus]] = {

        var toBeChecked = (new ArrayBuffer[String]()) ++ inFiles;
        var inputDirectories = List[FileStatus]();
        var inputFileStata = List[FileStatus]();

        while (!toBeChecked.isEmpty) {
            val glob = toBeChecked.head;
            toBeChecked.remove(0);
            val results = fs.globStatus(new Path(glob), pathFilter);
            if (results != null && !results.isEmpty) {
                for (flstts ← results) {
                    //TODO: find API to check if file is readable: flstts.getPermision.getUserAction.implies(FsAction.READ)
                    //println("working on "+flstts.getPath.toUri.getPath);
                    if (flstts.isDir) {
                        inputDirectories :+= flstts;
                        if (recursive)
                            toBeChecked += flstts.getPath.toUri.getPath+"/*";
                    } else {
                        inputFileStata :+= flstts;
                    }
                }
            }
        }
        //TODO: sort the directories and files so that it is advantageous to PieceMaker;

        //now iterate
        for (inputFile ← inputFileStata) {
            val fn = inputFile.getPath.toUri.getPath;
            if (fn != null) {
                pieceMaker.add(fn, inputFile.getLen);
            }
        }

        return (inputFileStata, inputDirectories);

    }

    abstract trait XmlCompatible[T] {
        def toXml : scala.xml.Elem;
        def fromXml(xml : scala.xml.Elem) : T;

    }

    trait XmlIOer[T <: XmlCompatible[_]] {
        lazy val _l = new HZLogger(classOf[XmlIOer[T]]);
        def writeData(fs : FileSystem, path : Path, t : T) {
            writeXml(fs, path, toXml(t));
        }

        def readData(fs : FileSystem, path : Path) : Option[T] = {
            readXml(fs, path) match {
                case None ⇒ None;
                case Some(x) ⇒ Some(fromXml(x));
            }
        }

        protected def readXml(fs : FileSystem, path : Path) : Option[scala.xml.Elem] = {

            try {
                val stat = fs.listStatus(path);

                if (stat == null || stat.length < 1) {
                    return None;
                }

                val size = stat(0).getLen.intValue;
                val buffer = new Array[Byte](size);
                val f = fs.open(path);

                //read the content
                val rl = f.read(0, buffer, 0, size);
                //                f.readFully(buffer);
                //                println("read "+rl+" bytes into a buffer of size "+buffer.length);
                f.close(); //freeup the handler
                val xmls = new String(buffer, "UTF-16");
                //                println("read this string '"+xmls+"'.");
                val xml = XML.loadString(xmls);
                if (xml != null) {
                    return Some(xml);
                } else {
                    return None;
                }
            } catch {
                case e : Exception ⇒
                    _l.error("Could not read xml", e);
                    return None;
            }

        }

        protected def writeXml(fs : FileSystem, path : Path, xml : scala.xml.Elem) {
            val file = fs.create(path);
            //println("At this point the xml is "+xml.toString);
            //println("At this point the xml is "+xml.toString.getBytes("UTF-16"));
            file.write(xml.toString.getBytes("UTF-16"));
            file.close();
        }

        protected def toXml(t : T) : scala.xml.Elem = t.toXml;
        protected def fromXml(xml : scala.xml.Elem) : T = {
            val ret = constructObject;
            ret.fromXml(xml);
            return ret;
        }

        /**
         * Derivers of this object should define this in the deriving object
         */
        def constructObject : T;
    }
}