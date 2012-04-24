package org.yagnus.hzip.pieces

import scala.xml._;
import org.yagnus.hzip.HdfsUtils.{ XmlIOer, XmlCompatible };
import org.apache.commons.lang.StringEscapeUtils

class FileSubpiece(fileName : String, thePieceId : Long, theStartOfPieceInCollection : Long, theStartOfPieceInKey : Long, pieceLen : Long)
    extends Subpiece[String](fileName, thePieceId, theStartOfPieceInCollection, theStartOfPieceInKey, pieceLen);

class FilePieceMaker
    extends PieceMakerImpl[String]
    with XmlCompatible[FilePieceMaker] {

    def toXml =
        <FilePieceSpec>
            {
                for (fn ← getAllKeys)
                    yield <FILE>
                              { <NAME>{ fn }</NAME><SIZE>{ getOriginalSize(fn) }</SIZE> }
                          </FILE>
            }
        </FilePieceSpec>

    def toConfigString : String = MessyXmlMesserUpper.xmlEscape(toXml.toString);

    def this(xml : scala.xml.Elem) {
        this();
        fromXml(xml);
    }

    def fromXml(xml : scala.xml.Elem) : FilePieceMaker = {
        xml match {
            case <FilePieceSpec>{ entries@_* }</FilePieceSpec> ⇒ {
                for (file@ <FILE>{ _* }</FILE> ← entries) {
                    val fn = (file \\ "NAME").text.trim;
                    val fs = BigDecimal((file \\ "SIZE").text.trim).toLong;
                    add(fn, fs);
                }
            }
        }
        return this;
    }

    def this(from : String) {
        this(XML.loadString(MessyXmlMesserUpper.xmlUnescape(from)));
    }

    protected def stripLeadingSlash(fn : String) = {
        if (fn.startsWith("/"))
            fn.slice(1, fn.length);
        else
            fn;
    }
    //small amount of normalization
    override def add(index : String, len : Long) {
        super.add(stripLeadingSlash(index), len);
    }
    override def getPieces(key : String) = super.getPieces(stripLeadingSlash(key));
}
object FilePieceMaker extends XmlIOer[FilePieceMaker] {
    type FilePieceMakerType = FilePieceMaker;
    def apply : FilePieceMaker = new FilePieceMaker();
    def apply(configString : String) : FilePieceMaker = new FilePieceMaker(configString);
    //some primitive object persistence code
    /*
     * To facilitate {@link HdfsUtils.XmlIOer#writeData}
     */
    def constructObject = new FilePieceMaker();

}

object MessyXmlMesserUpper {
    def xmlEscape(in : String) : String = {
        StringEscapeUtils.escapeXml(in.replace(",", "><><COMMA><><"));
    }

    def xmlUnescape(in : String) : String = {
        StringEscapeUtils.unescapeXml(in).replace("><><COMMA><><", ",");
    }

}