package org.dbpedia.spotlight.elasticsearch

import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.client.Client

/**
 * Configuration object for ElasticSearch.
 *
 * URI is stored in _id
 * Index types are ignored for now
 */
case class ESConfig(serverUrl: String,
                    port: Int,
                    index: String,
                    uriCountField: String,
                    sfField: String,
                    contextField: String,
                    timeoutMillis: Long) {

    val client: Client = makeClient

    private def makeClient = {
        val s = ImmutableSettings.settingsBuilder()
            .put("client.transport.ignore_cluster_name", true)
            .put("client.transport.ping_timeout", "15s")
        new TransportClient(s).addTransportAddress(new InetSocketTransportAddress(serverUrl, port))
    }

}
