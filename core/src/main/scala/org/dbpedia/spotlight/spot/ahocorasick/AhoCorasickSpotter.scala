/**
 * Copyright 2012
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dbpedia.spotlight.spot.ahocorasick

import collection.mutable.ListBuffer
import com.corruptmemory.aho_corasick.AhoCorasickBuilder.Data
import com.corruptmemory.aho_corasick.{Match, AhoCorasickBuilder}
import util.Sorting
import org.dbpedia.spotlight.spot.Spotter
import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.{SurfaceForm, Text, SurfaceFormOccurrence}
import scala.collection.JavaConversions._

/**
 * AhoCorasick Spotter Class
 *
 * @param builder  AhoCorasickBuilder instance
 * @param overlap  overlap: true or false?
 * @param pattern regex pattern to check if the chunk is a complete word
 */
class AhoCorasickSpotter(val builder: AhoCorasickBuilder[String], val overlap: Boolean, pattern: String = "\\s|\\n|\\t|[,.:;¿?¡!()'\"]") extends Spotter {

  private val LOG = LogFactory.getLog(this.getClass)
  private val finder = builder.build()
  private var name = ""

  LOG.debug("Allow overlap: " + overlap)

  /**
   * Find a specific text
   * @param text a text that you want spotting
   *
   */
  def extract(text: Text): java.util.List[SurfaceFormOccurrence] = {
    var results = finder.find(text.text);

    val buffer: ListBuffer[SurfaceFormOccurrence] = ListBuffer()

    if (overlap) {
      results.foreach(result =>
        buffer.append(new SurfaceFormOccurrence(new SurfaceForm(result.actual), text, result.start))
      )

    } else {
      filter(results, text.text, pattern).foreach(result =>
        buffer.append(new SurfaceFormOccurrence(new SurfaceForm(result.actual), text, result.start))
      )
    }


    seqAsJavaList[SurfaceFormOccurrence](buffer.toSeq)
  }


  /**
   * Comparator by Match start point
   */
  private object LengthOrdering extends Ordering[Match[String]] {
    def compare(matchA: Match[String], matchB: Match[String]) = matchA.start compare matchB.start
  }


  /**
   * When overlap is false, this method try to filter most relevant sequences
   *
   * E.g. Finding a text with the name Dilma Rousseff, Scala-aho-corasick found the follow sequences
   * Match(13,DI,Di,)
   * Match(13,D,D,)
   * Match(13,Dilma Rousseff,Dilma Rousseff,)
   * Match(13,Dilma,Dilma,)
   * Match(14,ILMA,ilma,)
   * Match(14,IL,il,)
   * Match(15,L,l,)
   * Match(15,LM,lm,)
   * Match(15,LMA,lma,)
   * Match(16,Ma,ma,)
   * Match(16,M,m,)
   * Match(17,a,a,)
   * Match(19,R,R,)
   * Match(19,Ro,Ro,)
   * Match(19,Rousseff,Rousseff,)
   * Match(19,Rousse,Rousse,)
   * Match(20,Ousse,ousse,)
   * Match(21,US,us,)
   * Match(21,USS,uss,)
   * Match(22,Sse,sse,)
   * Match(22,SS,ss,)
   * Match(22,S,s,)
   * Match(23,SEF,sef,)
   * Match(23,S,s,)
   * Match(24,Ef,ef,)
   * Match(25,F,f,)
   * Match(25,FF,ff,)
   * Match(26,F,f,)
   * Is relevant for me only the complete Match, like the third Match-  Match(13,Dilma Rousseff,Dilma Rousseff,)
   *
   * @param result
   * @param originalText
   * @param pattern
   * @return  a Seq of Match[T]
   */

  private def filter(result: Seq[Match[String]], originalText: String, pattern: String): Seq[Match[String]] = {

    var chunk: Match[String] = null
    var chunkStartPosition = 0
    var chunkLength = -1
    var lastChunk = -1;
    val buffer: ListBuffer[Match[String]] = ListBuffer()
    val resultsArray = result.toArray[Match[String]]
    //Sorting the array by Match start position
    Sorting.quickSort(resultsArray)(LengthOrdering)


    resultsArray.foreach(resultArray => {
      //Initialize the lastChunk position with the first Match
      if (lastChunk.equals(-1)) lastChunk = resultArray.start

      //Trying to get the bigger Match checking the chunk start position
      if (resultArray.actual.length > chunkLength &&
        resultArray.start >= chunkStartPosition &&
        lastChunk <= resultArray.start) {
        //Storing the good chunk
        chunk = resultArray
        chunkLength = resultArray.actual.length
        lastChunk = resultArray.start

      } else if (lastChunk != resultArray.start &&
        resultArray.start >= chunkStartPosition &&
        chunk !=null) {

        //Here chunkStart and lastChunk are setted to ignore Match's repetition
        // E.g
        // Match(13,Dilma Rousseff,Dilma Rousseff,)
        // Only sentences after 27º position is relevant when this chunk is selected as good chunk
        chunkStartPosition = chunk.start + chunk.actual.length

        // println(chunk)
        //Checking if is a complete word using a regex pattern
        if ((chunk.start == 0 || originalText.charAt(chunk.start - 1).toString.matches(pattern)) &&
            (chunkStartPosition >= originalText.length || originalText.charAt(chunkStartPosition).toString.matches(pattern)))
          buffer.append(chunk)

        chunkStartPosition= chunkStartPosition -1
        lastChunk = chunkStartPosition
        //reset control variables
        chunkLength = -1
        chunk = null
      }
    }
    )

    buffer.toSeq
  }

  /**
   * Every spotter has a name that describes its strategy
   * (for comparing multiple spotters during evaluation)
   */
  def getName(): String = {
    if (name == "") {
      val allMatches = if (overlap) "overlapping" else "non-overlapping"
      "AhoCorasickSpotter[" + allMatches + "]"
    } else {
      name
    }

  }

  def setName(newName: String) {
    name = newName;
  }
}

object AhoCorasickSpotter {
  /**
   * Build an  AhoCorasick trie from surface forms file
   * @param sfSet
   * @param caseSensitive  case sensitive: true or false?
   */
  def fromSurfaceForms(sfSet: Iterator[String], caseSensitive: Boolean): AhoCorasickBuilder[String] = {

    val buf: ListBuffer[Data[String]] = ListBuffer()

    sfSet.foreach(
      validChunk => buf.+=:(Data(validChunk, ""))
    )

    AhoCorasickBuilder.apply[String](buf.toSeq,
      if (caseSensitive)
        _.toChar
      else
        _.toLower
    )
  }

}
