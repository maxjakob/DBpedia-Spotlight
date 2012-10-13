package org.dbpedia.spotlight.evaluation

import org.dbpedia.spotlight.spot.Spotter
import org.dbpedia.spotlight.corpus.MilneWittenCorpus
import java.io.File
import org.dbpedia.spotlight.io.AnnotatedTextSource
import com.aliasi.dict.MapDictionary
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.util.Version
import org.dbpedia.spotlight.model.{Factory, SurfaceFormOccurrence}
import collection.JavaConversions
import org.apache.lucene.analysis._
import org.apache.commons.logging.LogFactory
import org.apache.lucene.analysis.standard.{StandardAnalyzer, ClassicAnalyzer}
import org.dbpedia.spotlight.spot.ahocorasick.AhoCorasickSpotter
import collection.mutable.ListBuffer

/**
 *
 *
 */
object EvalSpotterAhoCorasick {

  private val LOG = LogFactory.getLog(this.getClass)

  def main(args: Array[String]) {
    evalSpotting(MilneWittenCorpus.fromDirectory(new File("/home/mpiuser/temp/DBpediaSpotlightEvalData/milne-witten")))
    //evalSpotting(AnnotatedTextSource.fromOccurrencesFile(new File("/home/mpiuser/temp/DBpediaSpotlightEvalData/CSAWoccs.red-dis-3.7-sorted.tsv")))
  }

  def evalSpotting(annotatedTextSource: AnnotatedTextSource) {
    val analyzers = List(
      new SimpleAnalyzer(Version.LUCENE_36),
      new StopAnalyzer(Version.LUCENE_36),
      new ClassicAnalyzer(Version.LUCENE_36),
      new StandardAnalyzer(Version.LUCENE_36),
      new EnglishAnalyzer(Version.LUCENE_36),
      new WhitespaceAnalyzer(Version.LUCENE_36)
    )
    for (analyzer <- analyzers) {
      evalSpotting(annotatedTextSource, analyzer)
    }
  }

  def evalSpotting(annotatedTextSource: AnnotatedTextSource, analyzer: Analyzer) {
    // create gold standard and index
    var expected = Set[SurfaceFormOccurrence]()
    val dictionary = new MapDictionary[String]()
    val buf: ListBuffer[String] = ListBuffer()
    for (paragraph <- annotatedTextSource;
         occ <- paragraph.occurrences) {
      expected += Factory.SurfaceFormOccurrence.from(occ)
      buf.+=:(occ.surfaceForm.name)
    }

    //Config parameters
    //====================================================================================
    val caseSensitive = false
    val overlap = false
    //====================================================================================
    val spotter: Spotter = new AhoCorasickSpotter(AhoCorasickSpotter.fromSurfaceForms(buf.iterator, caseSensitive), overlap)
    //====================================================================================


    // eval
    var actual = Set[SurfaceFormOccurrence]()
    for (paragraph <- annotatedTextSource) {
      var results = spotter.extract(paragraph.text)
      actual = JavaConversions.asScalaBuffer(results).toSet union actual
    }

    printResults("AhoCorasickSpotter with %s and corpus %s".format(analyzer.getClass, annotatedTextSource.name),
      expected, actual)
  }


  def printResults(description: String, expected: Set[SurfaceFormOccurrence], actual: Set[SurfaceFormOccurrence]) {
    var truePositive = 0
    var falseNegative = 0
    for (e <- expected) {
      if (actual contains e) {
        truePositive += 1
      } else {
        falseNegative += 1
        LOG.debug("false negative: " + e)
      }
    }

    val falsePositive = actual.size - truePositive

    val precision = truePositive.toDouble / (truePositive + falseNegative)
    val recall = truePositive.toDouble / (truePositive + falsePositive)

    LOG.info(description)
    LOG.info(" | actual Y | actual N")
    LOG.info("expected Y | %3d | %3d".format(truePositive, falseNegative))
    LOG.info("expected N | %3d | N/A".format(falsePositive))
    LOG.info("precision: %f recall: %f".format(precision, recall))
    LOG.info("--------------------------------")
  }

}
