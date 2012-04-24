package org.yagnus
import java.io.{ FileInputStream, File }
import scala.util.Sorting

object SortFile extends App {
  println("Allocating a large buffer.");
  val buf = new Array[Byte]((2l * 1024 * 1024 * 1024 - 512).intValue);
  val in = new FileInputStream(new File(args(0)));
  println("Reading from "+args(0));
  val len=in.read(buf);
  println("Read in "+len+" bytes");
  var s = 0d;
  var zc=0l;
  for(b<-buf){
	s+=b;
	if(b==0)zc += 1;
  }
  println("The sum is "+s+", average is "+(s/len)+", zero count is "+zc);

  println("Now Sorting.");
  Sorting.quickSort(buf);

  println("Done.");

  /**
   * Sorting.quickSort(buf);
   * real	2m25.681s
   * user	2m4.425s
   * sys	0m1.703s
   */
  /**
   * Sorting.stableSort(buf);
   * real	19m48.563s
   * user	19m26.077s
   * sys	0m2.218s
   *
   */
}
