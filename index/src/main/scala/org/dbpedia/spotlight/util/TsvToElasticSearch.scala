package org.dbpedia.spotlight.util

import java.io.{PrintWriter, File}
import scala.io.Source
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.annotation.tailrec

/**
 * Prototypical transformation script for TSV style output to ElasticSearch
 * bulk input JSONs.
 *
 * The _id of the documents is the URI.
 *
 * For now, this does not include custom routing. The initial idea was to
 * spread the documents in a way, that is is unlikely for two concepts that
 * share a surface form to end up in the same shard. This might have the
 * advantage that work can be parallalized and work in the individual shards
 * is minimized. However, it would complicate the indexing process, since
 * we need to keep a mapping of surface forms to all shards that they have
 * been sent to. ElasticSearch uses the has of the ID plus a random sequence
 * to determine the shard, which should already give a good distribution.
 *
 * @author maxjakob
 */

case class ElasticSearchConfig(indexName: String, typeName: String, operationName: String)


case class DataPoint(id: String, fieldName: String, value: Any) {
  def merge(other: DataPoint) = {
    assert(id == other.id)
    assert(fieldName == other.fieldName)

    val newValue = value match {
      case l1: JSONArray => other.value match {
        case l2: JSONArray => new JSONArray(l1.list ::: l2.list)
        case v2: Any => new JSONArray(v2 :: l1.list)
      }
      case v1: Any => other.value match {
        case l2: JSONArray => new JSONArray(v1 :: l2.list)
        case v2: Any => new JSONArray(v1 :: v2 :: Nil)
      }
    }
    DataPoint(id, fieldName, newValue)
  }
}


abstract class BulkOperation(val indexName: String, val typeName: String, val dataPoint: DataPoint) {
  def toJson: String
  override def toString = toJson
}

class IndexOperation(indexName: String, typeName: String, dataPoint: DataPoint)
  extends BulkOperation(indexName: String, typeName: String, dataPoint: DataPoint) {
  def toJson: String = {
    val line1 = new JSONObject(Map( "index" ->
      new JSONObject(Map( "_index" -> indexName, "_type" -> typeName, "_id" -> dataPoint.id ))))
    val line2 = new JSONObject(Map( dataPoint.fieldName -> dataPoint.value ))
    line1 + "\n" + line2 + "\n"
  }
}

class UpdateOperation(indexName: String, typeName: String, dataPoint: DataPoint)
  extends BulkOperation(indexName: String, typeName: String, dataPoint: DataPoint) {
  def toJson: String = {
    val line1 = new JSONObject(Map( "update" ->
      new JSONObject(Map( "_index" -> indexName, "_type" -> typeName, "_id" -> dataPoint.id ))))
    val line2 = new JSONObject(Map( "doc" ->
      new JSONObject(Map( dataPoint.fieldName -> dataPoint.value ))))
    line1 + "\n" + line2 + "\n"
  }
}


object TsvToElasticSearch {

  val BULK_SIZE = 5000
  val TYPE_NAME = "entities"

  val SURFACE_FORM_FIELD = "SURFACE_FORM"
  val CONTEXT_FIELD = "CONTEXT"
  val URI_COUNT_FIELD = "URI_COUNT"

  private val TAB_SEPARATOR = "\t"

  // Utility function to not have 'close' calls somewhere else
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B = {
    try { f(param) } finally { param.close() }
  }

  def writeToFile(outFile: File, textData:String) = {
    using (new PrintWriter(outFile, "UTF-8")) { _.print(textData) }
  }

  def parseTokenCounts(line: String): DataPoint = {
    val Array(_occId, uri, sf, context, offset) = line.split(TAB_SEPARATOR)
    DataPoint(uri, CONTEXT_FIELD, context)
  }

  def parseSfCounts(line: String): DataPoint = {
    val Array(uri, sf) = line.split(TAB_SEPARATOR)
    DataPoint(uri, SURFACE_FORM_FIELD, sf)
  }

  val lineParsers = Map[String, String => DataPoint](
    "occs.uriSorted.tsv" -> parseTokenCounts,
    "surfaceForms.tsv" -> parseSfCounts
  )

  val bulkOpConstructors = Map[String, (String, String, DataPoint) => BulkOperation](
    "index" -> (new IndexOperation(_, _, _)),
    "update" -> (new UpdateOperation(_, _, _))
  )

  def isMergeable(d1: DataPoint, d2: DataPoint) = {
    (d1 != null &&) (d1.id == d2.id) && (d1.fieldName == d2.fieldName)
  }

  @tailrec
  def getDataPoints(parser: String => DataPoint,
                    lineIt: Iterator[String],
                    buffer: List[DataPoint],
                    bufferSizeLimit: Int):List[DataPoint] = {
    if (buffer.size >= bufferSizeLimit || !lineIt.hasNext) {
      return buffer
    }
    val dataPoint = parser(lineIt.next())
    val newBuffer = {
      if (buffer.nonEmpty && isMergeable(buffer.head, dataPoint)) {
        buffer.head.merge(dataPoint) :: buffer.tail
      }
      else {
        dataPoint :: buffer
      }
    }
    getDataPoints(parser, lineIt, newBuffer, bufferSizeLimit)
  }

  def tsvToJson(inFile: File, esConfig: ElasticSearchConfig, bulkSize: Int) = {
    val bulkOpConstructor = bulkOpConstructors.getOrElse(esConfig.operationName,
      throw new IllegalArgumentException("unknown operation '%s'".format(esConfig.operationName)))
    val parser: String => DataPoint = lineParsers.getOrElse(inFile.getName,
      throw new IllegalArgumentException("don't know how to parse '%s'".format(inFile.getName)))

    val iterator = Source.fromFile(inFile, "UTF-8").getLines()
    var i = 1
    while (iterator.hasNext) {
      val dataPoints = getDataPoints(parser, iterator, List.empty, bulkSize)
      val bulks = dataPoints.map(dp => bulkOpConstructor(esConfig.indexName, esConfig.typeName, dp))
      val outFile = new File(inFile.getAbsolutePath + "_%04d.json".format(i))
      writeToFile(outFile, bulks.mkString(""))
      i += 1
    }
  }

  def main(args : Array[String]) = {
    val indexName = args(0)
    val inputFileName = args(1)
    val operationName = args(2)

    val esConfig = ElasticSearchConfig(indexName, TYPE_NAME, operationName)

    tsvToJson(new File(inputFileName), esConfig, BULK_SIZE)
  }

}
