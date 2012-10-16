package org.dbpedia.spotlight.io

import org.dbpedia.extraction.sources.{XMLSource, Source}
import java.io.{InputStreamReader, FileInputStream, InputStream, File}
import org.dbpedia.extraction.util.Language
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.dbpedia.extraction.wikiparser.Namespace
import xml.{XML, Elem}

/**
 * Base class for classes creating OccurrenceSource objects from Wikipedia XML content
 */
abstract class WikipediaOccurrenceSourceFactory {

    protected def occurrenceSource(extractionSource: Source): OccurrenceSource

    /**
     * Loads from a dump file, compressed or uncompressed.
     */
    def fromXMLDumpFile(dumpFile : File, language: Language): OccurrenceSource = {
        var fileInputStream: InputStream = new FileInputStream(dumpFile)
        if (dumpFile.getName.endsWith(".bz2")) {
            fileInputStream = new BZip2CompressorInputStream(fileInputStream, true)
        }
        val inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8")
        occurrenceSource(XMLSource.fromReader(() => inputStreamReader, language, _.namespace == Namespace.Main))
    }

    /**
     * Loads from XML.
     */
    def fromXML(xml : Elem, language: Language): OccurrenceSource = {
        occurrenceSource(XMLSource.fromXML(xml, language))
    }

    /**
     * Loads from an XML string.
     */
    def fromXML(xmlString : String, language: Language): OccurrenceSource = {
        val xml : Elem = XML.loadString("<dummy>" + xmlString + "</dummy>")  // dummy necessary: when a string "<page><b>text</b></page>" is given, <page> is the root tag and can't be found with the command  xml \ "page"
        fromXML(xml, language)
    }

}

