package org.dbpedia.spotlight.evaluation

import org.dbpedia.spotlight.model.{DBpediaResourceOccurrence, DBpediaResource}
import org.dbpedia.extraction.util.WikiUtil
import org.dbpedia.spotlight.io.WikiOccurrenceSource
import org.dbpedia.spotlight.corpus.MilneWittenCorpus
import java.io.File

object EvalOfUri {

    val encoding = "utf-8"

    def main(args: Array[String]) {
        val rootDir = "/data/dbpedia-spotlight/evalPairCountDisambig/" // on server: /var/spotlight/model-quickstarter/wdir/
        val heldOutFileName = "heldout.txt"
        val pairCountFileName = "pairCounts"

        val mwDir = "/data/dbpedia-spotlight/MilneWitten"
        val enNerdStats = "/data/dicode/nerd-stats/nerd_stats_enwiki_20120601_5grams"

        val langs = List("da_DK", "de_DE", "es_ES", "fr_FR", "hu_HU", "it_IT", "nl_NL", "pt_BR", "ru_RU", "sv_SE", "tr_TR")

        for (lang <- langs) {
            System.err.println(lang + ": reading max p(uri|sf)")
            val maxOfUrisMap = maxOfUrisFromPairCounts(rootDir + lang + "/" + pairCountFileName)
            System.err.println("getting samples from max_p(uri|sf): " +
                maxOfUrisMap.get("Angela Merkel"), maxOfUrisMap.get("Barack Obama"))

            val fileSource = scala.io.Source.fromFile(rootDir + lang + "/" + heldOutFileName, encoding).getLines()
            val occSource = WikiOccurrenceSource.fromPigHeldoutFile(fileSource)

            System.err.println("evaluating " + lang)
            printEval(lang, maxOfUrisMap, occSource)
        }

        System.err.println("running for en_EN and MilneWitten")
        val maxOfUrisMapEn = maxOfUrisFromNerdStats(enNerdStats)
        val mw = MilneWittenCorpus.fromDirectory(new File(mwDir)).flatMap(_.occurrences)
        printEval("en_EN", maxOfUrisMapEn, mw)
    }

    def printEval(lang: String, maxOfUrisMap: Map[String, (String, Double)], occSource: Traversable[DBpediaResourceOccurrence]) {
        var correct = 0
        var notFound = 0
        var total = 0
        for (occ <- occSource) {
            maxOfUrisMap.get(occ.surfaceForm.name) match {
                case Some((uri, ofUri)) => {
                    if (occ.resource.uri == uri) {
                        correct += 1
                    }
                    else {
                        //println(occ.resource.uri + "\t" + uri + "\t" + ofUri)
                    }
                }
                case None => {
                    notFound += 1
                    //println("sf not in Wikipedia\t" + occ.surfaceForm.name)
                }
            }
            total += 1
        }

        println(lang + "\taccuracy: %d/%d = %.4f percent\tof which are 'No URI': %d = %.4f".format(
            correct, total, correct.toDouble/total, notFound, notFound.toDouble/total))
    }


    /**
     * @param fileName pairCounts file
     * @return map surfaceForm -> (uri, pUriGivenSF) for the uri that maximizes pUriGivenSf in the pairCounts file
     */
    def maxOfUrisFromPairCounts(fileName: String): Map[String, (String, Double)] = {
        val fileSource = scala.io.Source.fromFile(fileName, encoding).getLines()
        fileSource.foldLeft(Map[String, (String,Double)]()) { (m, line) =>
            val elements = line.split("\t")
            val sf = elements(0)
            val count = elements(2).toDouble
            val maxCount = m.getOrElse(sf, ("", 0.0))._2
            if (count > maxCount) {
                val label = WikiUtil.wikiDecode(elements(0).replaceFirst("http://\\w*\\.wikipedia.org/wiki/", ""))
                val uri = new DBpediaResource(label).uri
                m.updated(sf, (uri, count))
            }
            else {
                m
            }
        }
    }

    def maxOfUrisFromNerdStats(fileName: String): Map[String, (String, Double)] = {
        val fileSource = scala.io.Source.fromFile(fileName, encoding).getLines()
        fileSource.foldLeft(Map[String, (String,Double)]()) { (m, line) =>
            val elements = line.split("\t")
            val sf = elements(1)
            val ofUri = elements(2).toDouble
            val maxOfUri = m.getOrElse(sf, ("", 0.0))._2
            if (ofUri > maxOfUri) {
                val uri = new DBpediaResource(elements(0)).uri
                m.updated(sf, (uri, ofUri))
            }
            else {
                m
            }
        }
    }

}
