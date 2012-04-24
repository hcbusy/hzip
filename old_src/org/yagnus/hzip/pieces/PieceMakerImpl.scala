package org.yagnus.hzip.pieces

import scala.collection.mutable.ListBuffer;
import org.yagnus.scalasupport.maps._;
import org.yagnus.yadoop.Yadoop._;

class PieceMakerImpl[Key] extends PieceMaker[Key] {

    protected var mapping = new WrappedJavaLinkedHashMap[Key, ListBuffer[Piece]](10, 0.95f, false);

    protected var rMapping = new WrappedJavaHashMap[Long, ListBuffer[Subpiece[Key]]]();

    var curCollectionId = 0l;

    def add(index : Key, lengthInBytes : Long) {
        mapping.getOrSet(index, ListBuffer[Piece]()).append(new Piece(curCollectionId, 0, 0, lengthInBytes));
        rMapping.getOrSet(curCollectionId, ListBuffer[Subpiece[Key]]()).append(new Subpiece(index, 0, 0, 0, lengthInBytes));

        curCollectionId += 1;
    }

    // Gets the pieces associated with the key
    def getPieces(key : Key) = mapping.got(key);

    // Gets all keys
    def getAllKeys = mapping.keySet.iterator;
    def getKeyCount = mapping.size;
    
    // Given a piece, map the piece back to the original 
    def getCollection(collectionId : Long) = rMapping.got(collectionId);
    def getAllCollections = rMapping.keySet.iterator;
    def getCollectionCount = rMapping.size;
    
    def getOriginalSize(key : Key) : Long = {
        var ret = 0l;
        for (p ← mapping.get(key)) ret += p.lengthOfPiece;
        return ret;
    }
    
}