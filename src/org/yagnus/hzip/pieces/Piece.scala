package org.yagnus.hzip.pieces

class Piece(theCollectionId : Long, thePieceId : Long, theStartOfPieceInKey : Long, theLengthOfPiece : Long) {
    def collectionId = theCollectionId;
    def id = thePieceId;
    def startOfPieceInKey = theStartOfPieceInKey;
    def lengthOfPiece = theLengthOfPiece;
}

class Subpiece[Key](val theName : Key, val thePieceId : Long, val theStartOfPieceInCollection : Long, val theStartOfPieceInKey : Long, val pieceLen : Long) {
    def name = theName;
    def pieceId = thePieceId;
    def startOfPieceInCollection = theStartOfPieceInCollection;
    def startOfPieceInKey = theStartOfPieceInKey;
    def pieceLength = pieceLen;
}