package org.yagnus.hzip;

class CommonConfigs {

    def PROGRAM_CONFIG_FINAL_OUTPUT_DIR = "hzip.bw.final.output.dir";

    def PROGRAM_CONFIG_DECOMPRESS_START = "hzip.bw.start.of.decompression";
    def PROGRAM_CONFIG_ENCODING = "hzip.bw.encoding";

    def CONFIG_OUTPUT_PATH = "org.yagnus.hzip.output.path";
    
    def INTERNAL_JOBCONF_PIECE_SPEC = "org.yagnus.hzip.pieces.PieceSepc";

    def INPUT_FILE_SIZE = "input.file.size";

    def currentVersion = "0.0.4";

    def OUTPUT_HZ_MANIFEST_FILE = "/meta.manifest";
    def OUTPUT_HZ_META_DATA = "meta.data";
    def OUTPUT_HZ_DATA_FILE = "data";

    def INTERNAL_HZIP_GROUP_NAME = "hzip";
    
    def GLOBAL_TEMP="/tmp";
    def LOCAL_TEMP="tmp";

}

object Common extends CommonConfigs;