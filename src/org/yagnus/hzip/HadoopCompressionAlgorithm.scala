package org.yagnus.hzip;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;

/**
 * This is the API through which compression algorithms are invoked.
 * The method comperssFile returns a JobConf corresponding to the 
 * Hadoop work that needs to be done to compress input file into the 
 * output file.
 * 
 * The contract is that inputFileName specifiles the name of one file, outputFileName is a
 * directory where the compressor will output the data. The caller is responsible for creating
 * and removing the tempFileName directory on hdfs  before and after invoking the 
 * createCompressionJob method. if tempPathName is null, the value will default to /tmp.
 * 
 * @author hc.busy
 *
 */
abstract class HadoopCompressionAlgorithm(fs:FileSystem){
	/**
	 * 
	 * @param inputFileName   input file name
	 * @param outputPathName  output path name
	 * @param tempPathName    the directory to use to store intermediate data
	 * @return a JobConf to compress the files, null if an error or compression could not attempted.
	 */
	
  
   
  def createCompressionJob(): CompressionJob;
}
class CompressionJob(job:JobConf){val hadoopJob=job:JobConf; def preHadoop()={} ; def postHadoop()={};};