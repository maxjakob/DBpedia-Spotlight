package org.dbpedia.spotlight.spot.fst

import org.dbpedia.spotlight.model.{SurfaceFormOccurrence, Text, SurfaceForm}
import org.apache.lucene.util.fst._
import org.apache.lucene.util. IntsRef
import io.Source
import org.dbpedia.spotlight.spot.Spotter
import scala.collection.JavaConversions._
import org.apache.commons.logging.LogFactory

/**
 * Memory-friendly Spotter implementation that might be hard on
 * the CPU. Produces all N-grams in a sentence and checks a Lucene
 * finite state automaton if this N-gram is in the lexicon.
 *
 * @param surfaceForms list of SurfaceForm as dictionary
 */
class FsaSpotter(surfaceForms: List[SurfaceForm]) extends Spotter {

    private val LOG = LogFactory.getLog(this.getClass)

    var name = "FstSpotter"

    val fstBuilder = new Builder(FST.INPUT_TYPE.BYTE4, NoOutputs.getSingleton)

    LOG.info("Indexing " + name)
    surfaceForms.map(_.name).map(toIntsRef).sorted.foreach {
        fstBuilder.add(_, NoOutputs.getSingleton)
    }

    val fst = fstBuilder.finish()

    def toIntsRef(sf: String) = Util.toUTF32(sf, new IntsRef(sf.length))

    def extract(text: Text): java.util.List[SurfaceFormOccurrence] = {
        var l = List[SurfaceFormOccurrence]()
        for (i <- 5 to 1 by -1) {
            for (ngram <- text.text.split(" ").sliding(i)) { //TODO proper tokenizer
                val s = ngram.mkString(" ")                  //TODO get from text using offsets
                if (Util.get(fst, toIntsRef(s)) != null) {
                    val occ = new SurfaceFormOccurrence(new SurfaceForm(s), text, 0) //TODO offset
                    l = occ :: l
                }
            }

        }
        l
    }

    def getName = name

    def setName(n: String) {
        name = n
    }

}
