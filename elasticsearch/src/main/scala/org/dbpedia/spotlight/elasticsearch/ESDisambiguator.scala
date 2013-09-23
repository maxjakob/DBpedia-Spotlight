package org.dbpedia.spotlight.elasticsearch

import org.dbpedia.spotlight.model._
import org.elasticsearch.action.search.{MultiSearchResponse, SearchRequestBuilder}
import org.elasticsearch.index.query.{MatchQueryBuilder, QueryBuilders}
import org.elasticsearch.search.SearchHits

/**
 * Disambiguator that uses ElasticSearch
 */
class ESDisambiguator(config: ESConfig) extends ParagraphDisambiguator {

    def name = """ElasticSearchDisambiguator"""

    def disambiguate(paragraph: Paragraph): List[DBpediaResourceOccurrence] = {
        bestK(paragraph, 1).flatMap(_._2).toList
    }

    def bestK(paragraph: Paragraph, k: Int): Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]] = {
        val multiSearchBuilder = config.client.prepareMultiSearch()
        for (sfOcc <- paragraph.occurrences) {
            val srb = config.client.prepareSearch()
                .setQuery(getQuery(sfOcc))
                .addFields(config.sfField, config.uriCountField)
                .setSize(k)
            multiSearchBuilder.add(srb)
        }

        val responses = multiSearchBuilder.execute().actionGet(config.timeoutMillis)
        createResultMap(responses, paragraph, k)
    }

    private def getQuery(sfOcc: SurfaceFormOccurrence) = {
        QueryBuilders.boolQuery()
            .must(
                QueryBuilders.fuzzyQuery(config.sfField, sfOcc.surfaceForm.name)
                    .minSimilarity(0.9f)
            )
            .should(
                QueryBuilders.matchQuery(config.contextField, sfOcc.context.text)
                    .operator(MatchQueryBuilder.Operator.AND)
            )
    }

    private def createResultMap(responses: MultiSearchResponse, paragraph: Paragraph, k: Int) = {
        val resultMap = collection.mutable.Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]]()
        val sfOccsIt = paragraph.getOccurrences().iterator()

        for (response <- responses.getResponses) {
            val sfOcc = sfOccsIt.next()
            val hits = response.getResponse.getHits
            if (hits.totalHits() > 0) {
                val resources = makeResources(hits, k)
                val resOcc = resources.map( res =>
                    new DBpediaResourceOccurrence(res, sfOcc.surfaceForm, sfOcc.context, sfOcc.textOffset)
                )
                resultMap.put(sfOcc, resOcc)
            }
        }

        resultMap.toMap
    }

    private def makeResources(hits: SearchHits, k: Int) = {
        val resultBuffer = collection.mutable.ListBuffer[DBpediaResource]()
        for (i <- 0 to math.min(hits.totalHits().toInt, k) - 1) {
            val hit = hits.getAt(i)
            val uri = hit.id()
            val support = hit.field(config.uriCountField).value[Int]()

            resultBuffer.append(new DBpediaResource(uri, support))
        }
        resultBuffer.result()
    }
}
