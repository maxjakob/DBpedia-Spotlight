package org.dbpedia.spotlight.elasticsearch

import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.settings.ImmutableSettings

/**
 * Configuration object for ElasticSearch.
 *
 * URI is stored in _id
 * Index types are ignored for now
 */
case class ESConfig(serverUrl: String,
                    index: String,
                    uriCountField: String,
                    sfField: String,
                    contextField: String,
                    timeoutMillis: Long) {

    val client = makeClient

    private def makeClient = {
        val nb = NodeBuilder.nodeBuilder()
        val s = ImmutableSettings.settingsBuilder().put("network.host", serverUrl)
        nb.settings(s).clusterName("DBpedia Spotlight disambiguation").build().client()
    }

}
