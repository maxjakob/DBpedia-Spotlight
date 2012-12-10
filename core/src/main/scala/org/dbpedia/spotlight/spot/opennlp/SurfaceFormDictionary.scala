package org.dbpedia.spotlight.spot.opennlp

import java.io.{FileInputStream, File}
import collection.mutable.HashSet
import org.dbpedia.spotlight.util.bloomfilter.LongFastBloomFilter
import com.aliasi.util.AbstractExternalizable
import com.aliasi.dict.Dictionary
import scala.collection.JavaConversions._
import org.apache.lucene.util.fst.{Builder, FST, NoOutputs, Util}
import org.apache.lucene.store.InputStreamDataInput
import org.apache.lucene.util.IntsRef

/**
 * @author Joachim Daiber
 */


/**
 * A surface form dictionary (or set, lookup) can be used to check if a surface form
 * is known to be a valid spot candidate.
 */
abstract class SurfaceFormDictionary(caseSensitive: Boolean = true) {
  def contains(surfaceForm: String): Boolean
  def add(surfaceForm: String)
  def normalizeEntry(entry: String) = if(caseSensitive) entry else entry.toLowerCase
  def size: Int
}

/**
 * This is a SurfaceForm dictionary using a HashSet, hence it is fully reliable but requires more space
 * than a fuzzy dictionary.
 */
class ExactSurfaceFormDictionary(caseSensitive: Boolean = true) extends SurfaceFormDictionary(caseSensitive) {
  val surfaceFormDictionary = new HashSet[String]

  def add(surfaceForm: String) {
    surfaceFormDictionary += normalizeEntry(surfaceForm)
  }
  def contains(surfaceForm: String): Boolean = surfaceFormDictionary.contains(normalizeEntry(surfaceForm))
  def size = surfaceFormDictionary.size
}

/**
 * This is a SurfaceForm dictionary using a Bloom filter. The properties of Bloom filters are very helpful
 * here. There will never be false negatives, but false positives may occur with a probability
 * that can be specified when creating the BloomFilter.
 */
class ProbabilisticSurfaceFormDictionary(expectedSize: Int, caseSensitive: Boolean = true, falsePositiveProbability: Double = 0.01)
  extends SurfaceFormDictionary(caseSensitive) {

  val bloomFilter: LongFastBloomFilter =
    LongFastBloomFilter.getFilter(expectedSize, falsePositiveProbability)

  def add(surfaceForm: String) {
    bloomFilter.add(normalizeEntry(surfaceForm).getBytes)
  }
  def contains(surfaceForm: String) = bloomFilter.contains(normalizeEntry(surfaceForm).getBytes)
  def size = bloomFilter.getCurrentNumberOfElements.toInt
}


class FsaSurfaceFormDictionary(expectedSize: Int, caseSensitive: Boolean) extends SurfaceFormDictionary {
    private val fsaBuilder = new Builder(FST.INPUT_TYPE.BYTE4, NoOutputs.getSingleton)
    private var fsa: FST[Object] = null
    private var lastSurfaceForm = ""
    private var count = 0

    def this(fsa: FST[Object], caseSensitive: Boolean) {
        this(0, caseSensitive)
        this.fsa = fsa
    }

    private def toIntsRef(sf: String) = Util.toUTF32(normalizeEntry(sf), new IntsRef(sf.length))

    def add(surfaceForm: String) {
        require(lastSurfaceForm < surfaceForm, "surface forms have to be added in sorted order")
        require(count < expectedSize, "expected number of surface forms was already added")
        fsaBuilder.add(toIntsRef(surfaceForm), NoOutputs.getSingleton)
        count += 1
        if (count == expectedSize) {
            fsa = fsaBuilder.finish()
        }
        lastSurfaceForm = surfaceForm
    }

    def contains(surfaceForm: String): Boolean = {
        Util.get(fsa, toIntsRef(normalizeEntry(surfaceForm))) != null
    }

    def size = count
}


/**
 * Simple factory methods for each of the SurfaceFormDictionary implementations.
 */
object SurfaceFormDictionary {
  def fromIterator(entries: scala.collection.Iterator[String],
                   surfaceformDictionary: SurfaceFormDictionary = new ExactSurfaceFormDictionary())
    : SurfaceFormDictionary = {

    entries.foreach(line => surfaceformDictionary.add(line))
    surfaceformDictionary
  }
  def fromLingPipeDictionary(dictionary: Dictionary[String],
                             surfaceformDictionary: SurfaceFormDictionary = new ExactSurfaceFormDictionary()) = {

    dictionary.entryList().foreach(entry => surfaceformDictionary.add(entry.phrase()))
    surfaceformDictionary
  }

  def fromLingPipeDictionaryFile(dictionaryFile: File, caseSensitive: Boolean = true): SurfaceFormDictionary = {
    fromLingPipeDictionary(AbstractExternalizable.readObject(dictionaryFile).asInstanceOf[Dictionary[String]])
  }

}

object ProbabilisticSurfaceFormDictionary {
  def fromFile(dictionaryFile: File, caseSensitive: Boolean = true) : SurfaceFormDictionary = {
    SurfaceFormDictionary.fromIterator(io.Source.fromFile(dictionaryFile).getLines(),
      new ProbabilisticSurfaceFormDictionary(io.Source.fromFile(dictionaryFile).size, caseSensitive))
  }
  def fromLingPipeDictionaryFile(dictionaryFile: File, caseSensitive: Boolean = true): SurfaceFormDictionary = {
    val lingpipeDictionary: Dictionary[String] = AbstractExternalizable.readObject(dictionaryFile).asInstanceOf[Dictionary[String]]
    SurfaceFormDictionary.fromLingPipeDictionary(lingpipeDictionary,
      new ProbabilisticSurfaceFormDictionary(lingpipeDictionary.size(), caseSensitive))
  }
    def fromLingPipeDictionary(lingpipeDictionary: Dictionary[String], caseSensitive: Boolean = true): SurfaceFormDictionary = {
        SurfaceFormDictionary.fromLingPipeDictionary(lingpipeDictionary,
            new ProbabilisticSurfaceFormDictionary(lingpipeDictionary.size(), caseSensitive))
    }
}

object ExactSurfaceFormDictionary {
  def fromFile(dictionaryFile: File, caseSensitive: Boolean = true) : SurfaceFormDictionary = {
    SurfaceFormDictionary.fromIterator(io.Source.fromFile(dictionaryFile).getLines(),
      new ExactSurfaceFormDictionary(caseSensitive))
  }
  def fromLingPipeDictionary(dictionaryFile: File, caseSensitive: Boolean = true) : SurfaceFormDictionary = {
    SurfaceFormDictionary.fromLingPipeDictionary(AbstractExternalizable.readObject(dictionaryFile).asInstanceOf[Dictionary[String]],
      new ExactSurfaceFormDictionary(caseSensitive))
  }
}

object FsaSurfaceFormDictionary {
    def fromFile(dictionaryFile: File, caseSensitive: Boolean = true) : SurfaceFormDictionary = {
        SurfaceFormDictionary.fromIterator(io.Source.fromFile(dictionaryFile).getLines().toList.sorted.toIterator,
            new FsaSurfaceFormDictionary(io.Source.fromFile(dictionaryFile).size, caseSensitive))
    }
    def fromFsaFile(fsaFile: File, caseSensitive: Boolean = true) : SurfaceFormDictionary = {
        val dataIn = new InputStreamDataInput(new FileInputStream(fsaFile))
        val fsa = new FST[Object](dataIn, NoOutputs.getSingleton)
        new FsaSurfaceFormDictionary(fsa, caseSensitive)
    }
    def fromLingPipeDictionaryFile(dictionaryFile: File, caseSensitive: Boolean = true): SurfaceFormDictionary = {
        val d = AbstractExternalizable.readObject(dictionaryFile).asInstanceOf[Dictionary[String]]
        SurfaceFormDictionary.fromIterator(d.entryList().map(_.phrase).sorted.toIterator,
            new FsaSurfaceFormDictionary(d.size(), caseSensitive))
    }
    def saveFsa(fsaSurfaceFormDictionary: FsaSurfaceFormDictionary, file: File) {
        fsaSurfaceFormDictionary.fsa.save(file)
    }
}



