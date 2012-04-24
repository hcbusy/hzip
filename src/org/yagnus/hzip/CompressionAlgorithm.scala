package org.yagnus.hzip;
import scala.reflect.BeanProperty
import org.apache.hadoop.fs.FileSystem
import org.yagnus.hzip.io.HZVer
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import scala.collection.GenSeq

//An algorithm contains pre- and post- hadoop steps and a configuration of how to run
trait CompressionAlgorithmStep {
  def preHadoop {}
  def getHadoop: Job = null;
  def postHadoop {}

  def getDisplayName: String = getClass().getSimpleName();
}

/* The factory just needs to be customizable and returns a bunch of steps.
 * Note the factory should pre-customize each step before returning them 
 * so they are ready to execute.
 */
class CompressionAlgorithmFactory extends HZVer {
  var op, input, output, temp: String = _;
  var fs: FileSystem = _;
  var conf: Configuration = null;
  var debugLevel: Int = 0;
  def init(op: String, input: String, output: String, temp: String, conf: Configuration, fs: FileSystem) {
    this.op = op;
    this.input = input;
    this.output = output;
    this.temp = temp;
    this.fs = fs;
    this.conf = conf;
    this.debugLevel = conf.getInt(RuntimeConstants.conf.DEBUG_LEVEL, 0);
  }
  def directorifyInput = if (!input.endsWith("/")) input += "/";
  def directorifyOutput = if (!input.endsWith("/")) output += "/";
  def directorifyIO = { directorifyInput; directorifyOutput; }

  def getCompressionAlgorithm: GenSeq[CompressionAlgorithmStep] = Seq();
  def getDecompressionAlgorithm: GenSeq[CompressionAlgorithmStep] = Seq();
  def parseCommandline(params: Seq[String]): Option[CompressionAlgorithmFactory] = None;
  def getDisplayName: String = getClass().getSimpleName();
  def skipHzStamp = false;
}
