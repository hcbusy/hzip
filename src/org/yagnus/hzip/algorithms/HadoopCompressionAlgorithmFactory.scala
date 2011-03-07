package org.yagnus.hzip.algorithms;

import org.apache.hadoop.fs.FileSystem;

/**
 * 
 * This is the trait that generates algorithms from configuration parameters
 * 
 * implementers please write a constructor requiring zero parameters.
 * 
 * @author hc.busy
 *
 */
trait HadoopCompressionAlgorithmFactory{

    /*
     * configureFromParameter will receive all strings from commandline. This method will return true 
     * if it successfully generated an algorithm from the commnad line, otherwise false;
     * 
     * @returns None if the current arguments are not supported by the current factory, 
     *  an algorithm fully configured if they are
     */
    def parseCommandline(fs : FileSystem, args : Array[String]) : Option[HadoopCompressionAlgorithm];
    
    /**
     * This method should be implemented by deriver and indicate if the commandline is 
     * relevant to the current factory.
     * 
     * @param args the command line arguments
     * @return true if applicable, false if not.
     */
    def parametersAreAplicable(args:Array[String]):Boolean;
    
}
