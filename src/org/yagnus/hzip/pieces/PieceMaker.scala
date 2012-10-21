package org.yagnus.hzip.pieces

/**
 * Piece maker makes pieces, each of which will be compressed by a block compressor.
 * The input will typically be a string of file names followed by the length. It
 * will then be split or re-assembled
 * 
 * @author hc.busy
 *
 */
abstract class PieceMaker[Key] {

    def add(index : Key, lengthInBytes : Long) : Unit;

    // Gets the pieces associated with the key
    def getPieces(key : Key) : Option[Seq[Piece]];

    def getKeyCount : Long;

    // Gets all keys
    def getAllKeys : Iterator[Key];

    // Given a piece, map the piece back to the original 
    def getCollection(collectionId : Long) : Option[Seq[Subpiece[Key]]];

    //give all the collection id's
    def getAllCollections : Iterator[Long];
    def getCollectionCount : Long;

    def getOriginalSize(key : Key) : Long;
}