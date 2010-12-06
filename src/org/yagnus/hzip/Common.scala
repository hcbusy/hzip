package org.yagnus.hzip;

class CommonConfigs {

    def PROGRAM_CONFIG_FINAL_OUTPUT_DIR = "hzip.bw.final.output.dir";

    def PROGRAM_CONFIG_DECOMPRESS_START = "hzip.bw.start.of.decompression";
    def PROGRAM_CONFIG_ENCODING = "hzip.bw.encoding";

    def CONFIG_WORD_SIZE = "org.yagnus.hzip.word.size";
    def CONFIG_OUTPUT_PATH = "org.yagnus.hzip.output.path";

    def INPUT_FILE_SIZE = "input.file.size";

    def currentVersion = "0.0.3";

    def INTERNAL_JOBCONF_BATCH_SPEC = "org.yagnus.hzip.internal.batch.spec";
    def INTERNAL_JOBCONF_BATCH_TMP = "org.yagnus.hzip.internal.batch.tmp.path";

    // all file names are prepended with '/'
    def INTERNAL_HZ_BATCH_FILES = "/batches";

    def OUTPUT_HZ_MANIFEST_FILE = "/meta.manifest";
    def OUTPUT_HZ_MISC_FILE = "meta.misc";
    def OUTPUT_HZ_DATA_FILE = "/data";

    def INTERNAL_HZIP_GROUP_NAME = "hzip";
    def INTERNAL_COUNTER_BATCH_COUNT = "org.yagnus.hzip.batch.batch.progress.";


}

object Common extends CommonConfigs;