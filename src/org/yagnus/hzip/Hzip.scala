package org.yagnus.hzip;

import org.yagnus.hzip.ThirdParty._;
import org.yagnus.yadoop.Yadoop._;
import org.yagnus.yadoop.Cmp;

import org.apache.hadoop.fs._;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;

import scala.collection.mutable.{ HashSet, ArrayBuffer };

object Hzip {
    def main(args : Array[String]) : Unit = {

        val tempDir = "/tmp";

        import Actions._;
        import Algos._;
        println("Welcome to HZip, high quality compression for hadoop.(version "+Common.currentVersion+")");

        if (args.length < 1) {
            println("Please enter the algorithm type.");
            exit(1);
        }
        val (algo, action) = args(0) match {
            case "gunzip" ⇒
                (gzip, Decompress)
            case "gzip" ⇒ {
                println("Not implemented yet "+args(0));
                exit(2);
                (gzip, Compress)
            }
            case "bunzip" ⇒
                (bzip, Decompress)
            case "bzip" ⇒ {
                println("Not implemented yet "+args(0));
                exit(2);
                (bzip, Compress)
            }
            case "unzip" ⇒ {
                (zip, Decompress)
            }

            case "zip" ⇒ {
                println("Not implemented yet "+args(0));
                exit(2);
                (zip, Compress)
            }
            case "cmp" ⇒ {
                (null, Compare);
            }
            case _ ⇒ {
                println("Unknown compression library "+args(0));
                exit(2);
                (null, null);
            }
        }

        val fs = FileSystem.get(new Configuration());

        if (action == Compare) {
            //Perform compression.

            if (args.length != 3) {
                println("Must specify two files to compare.");
                usage();
                exit(3);
            }
            println("Comparing files '"+args(1)+"' and '"+args(2)+"'");

            exit(Cmp.diff(fs, args(1), args(2)));

        } else if (action == Decompress) {
            var todo : Option[HadoopCompressionAlgorithm] = None;

            if (algo == gzip || algo == bzip || algo == zip) {
                val gPaths = new ArrayBuffer[Path]();
                val oFiles = new ArrayBuffer[String]();

                val seen = new HashSet[String]();
                for {
                    inFn ← args.drop(1)
                    if (inFn.endsWith(".gz") || inFn.endsWith(".zip") || inFn.endsWith(".bz2"))
                    if !seen.contains(inFn)
                } {

                    var outFn : String = null;
                    if (inFn.endsWith(".gz")) outFn = inFn.slice(0, inFn.length - 3)
                    else outFn = inFn.slice(0, inFn.length - 4)

                    val inReady = fs.exists(inFn);
                    val outReady = !fs.exists(outFn);

                    if (inReady && outReady) {
                        gPaths.append(inFn);
                        oFiles.append(outFn);
                    }
                    seen.add(inFn);
                }

                if (gPaths.length == 0) {
                    println("There wasn't any file in the list that ended with the right extension");
                }
                //println("I've just decompressed '"+infile+"' using algorithm "+algo);
                if (algo == gzip) {
                    todo = Some(new GzipDecompression(fs, gPaths, oFiles, tempDir));
                } else if (algo == bzip) {
                    todo = Some(new Bzip2Decompression(fs, gPaths, oFiles, tempDir));

                }
            }

            todo match {
                case None ⇒
                    println("There is nothing to do.");
                    exit(6);
                case Some(task) ⇒ {
                    val taskDetail = task.createCompressionJob;
                    println("Preparing...");
                    taskDetail.preHadoop();
                    println("Now running hadoop job...");
                    JobClient.runJob(taskDetail.hadoopJob);
                    println("Now finalizing comprsesion...");
                    taskDetail.postHadoop();
                    println("done.");
                }
            }

        }
    }

    def usage() {
        println("TODO: type the usage instructions.");
        println("gunzip, cmp");
    }
    def exit(code : Int) = {
        usage();
        System.exit(code);
    }

    object Algos extends Enumeration {
        val gzip, zip, bzip, hzip = Value;
    }

    object Actions extends Enumeration {
        val Compress, Decompress, Compare = Value;
    }
}

