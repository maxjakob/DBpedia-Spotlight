package org.dbpedia.spotlight.elasticsearch

import org.dbpedia.spotlight.model._
import org.elasticsearch.action.search.{MultiSearchResponse, SearchRequestBuilder}
import org.elasticsearch.index.query.QueryBuilders

/**
 * Disambiguator that uses ElasticSearch
 */
class ESDisambiguator(config: ESConfig) extends ParagraphDisambiguator {

    def name = """ElasticSearchDisambiguator"""

    def disambiguate(paragraph: Paragraph): List[DBpediaResourceOccurrence] = {
        bestK(paragraph, 1).flatMap(_._2).toList
    }

    def bestK(paragraph: Paragraph, k: Int): Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]] = {
        val occsMap = collection.mutable.Map[String, Set[SurfaceFormOccurrence]]()

        val multiSearchBuilder = config.client.prepareMultiSearch()
        for (sfOcc <- paragraph.occurrences) {
            val srb = config.client.prepareSearch()
                .setQuery(getQuery(sfOcc))
                .addFields(config.sfField, config.uriCountField)
                .setSize(k)
            multiSearchBuilder.add(srb)

            val sf = sfOcc.surfaceForm.name  //TODO normalize sfs!
            occsMap.put(sf, occsMap.getOrElse(sf, Set()) + sfOcc)
        }

        val responses = multiSearchBuilder.execute().actionGet(config.timeoutMillis)
        createResultMap(responses, occsMap, k)
    }

    private def getQuery(sfOcc: SurfaceFormOccurrence) = {
        //TODO return a Query object
        //TODO be fuzzy for surface forms
        QueryBuilders.queryString(config.sfField+":\""+sfOcc.surfaceForm.name+"\" "+config.contextField+":\""+sfOcc.context.text+"\"")

    }

    private def createResultMap(responses: MultiSearchResponse, occsMap: collection.mutable.Map[String, Set[SurfaceFormOccurrence]], k: Int) = {
        val resultMap = collection.mutable.Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]]()

        val allHits = responses.getResponses.map(_.getResponse.getHits)
        for (hits <- allHits.filter(_.totalHits() > 0)) {

            val resultBuffer = collection.mutable.ListBuffer[DBpediaResource]()
            for (i <- 0 to math.min(hits.totalHits().toInt, k) - 1) {
                val hit = hits.getAt(i)
                val uri = hit.id()
                val support = hit.field(config.uriCountField).value[Int]()

                resultBuffer.append(new DBpediaResource(uri, support))
            }

            val sfOccs: Set[SurfaceFormOccurrence] = occsMap(hits.getAt(0).field(config.sfField).getValue[String]())
            for (sfOcc <- sfOccs) {
                if (!resultMap.contains(sfOcc)) {
                    val resOccs = resultBuffer.map( res =>
                        new DBpediaResourceOccurrence(res, sfOcc.surfaceForm, sfOcc.context, sfOcc.textOffset)
                    ).result()
                    resultMap.put(sfOcc, resOccs)
                }
            }
        }

        resultMap.toMap
    }
}
