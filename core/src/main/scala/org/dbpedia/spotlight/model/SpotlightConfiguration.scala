/*
 * Copyright 2012 DBpedia Spotlight Development Team
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  Check our project website for information on how to acknowledge the authors and how to contribute to the project: http://spotlight.dbpedia.org
 */
package org.dbpedia.spotlight.model

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.BooleanQuery
import org.dbpedia.spotlight.exceptions.ConfigurationException
import java.io._
import java.net.URI
import java.net.URISyntaxException
import java.util._
import org.apache.commons.lang.NotImplementedException

/**
 * Holds all configuration parameters needed to run the DBpedia Spotlight Server
 * Reads values from a config file
 * <p/>
 * (TODO) and should validate if the inputs are acceptable, failing gracefully and early.
 * (TODO) break down configuration into smaller pieces
 *
 * @author pablomendes
 * @author maxjakob
 */
case class SpotlightConfiguration(configFileName: String,
                                  dbpediaOntologyNamespace: String,
                                  language: String,
                                  i18nLanguageCode: String,
                                  contextIndexDirectory: String,
                                  candidateMapDirectory: String,
                                  candidateMapInMemory: Boolean,
                                  similarityThresholds: List[Double],
                                  similarityThresholdsFile: String,
                                  taggerFile: String,
                                  stopWordsFile: String,
                                  stopWords: Set[String],
                                  serverURI: String,
                                  sparqlMainGraph: String,
                                  sparqlEndpoint: String,
                                  maxCacheSize: Long,
                                  dbpediaResourceFactory: DBpediaResourceFactory,
                                  analyzer: Analyzer,
                                  spotterConfiguration: SpotterConfiguration,
                                  disambiguatorConfiguration: DisambiguatorConfiguration) {
}


object SpotlightConfiguration {

    private val LOG = LogFactory.getLog(SpotlightConfiguration.getClass)

    private var INSTANCE: SpotlightConfiguration = null


    def apply(configFileName: String) {
        this.configFileName = configFileName

        val config = new Properties
        try {
            config.load(new FileInputStream(new File(configFileName)))
        }
        catch {
            case e: IOException => {
                throw new ConfigurationException("Cannot read configuration file " + configFileName, e)
            }
        }
        dbpediaResourceNamespace = getProp(config, "org.dbpedia.spotlight.namespace.resource", "org.dbpedia.spotlight.default_namespace", DEFAULT_RESOURCE_NAMESPACE)
        dbpediaOntologyNamespace = getProp(config, "org.dbpedia.spotlight.namespace.ontology", "org.dbpedia.spotlight.default_ontology", DEFAULT_ONTOLOGY_NAMESPACE)
        i18nLanguageCode = config.getProperty("org.dbpedia.spotlight.language_i18n_code", DEFAULT_LANGUAGE_I18N_CODE)
        spotterConfiguration = new SpotterConfiguration(configFileName)
        disambiguatorConfiguration = new DisambiguatorConfiguration(configFileName)
        contextIndexDirectory = disambiguatorConfiguration.contextIndexDirectory
        candidateMapDirectory = config.getProperty("org.dbpedia.spotlight.candidateMap.dir", "").trim
        if (candidateMapDirectory == null || !new File(candidateMapDirectory).isDirectory) {
            LOG.warn("Could not use the candidateMap.dir provided. Will use index.dir both for context and candidate searching.")
            candidateMapDirectory = contextIndexDirectory
        }
        candidateMapInMemory = (config.getProperty("org.dbpedia.spotlight.candidateMap.loadToMemory", "false").trim == "true")
        try {
            val r: BufferedReader = new BufferedReader(new FileReader(new File(contextIndexDirectory, similarityThresholdsFile)))
            var line: String = null
            similarityThresholds = new ArrayList[Double]
            while ((({
                line = r.readLine; line
            })) != null) {
                similarityThresholds.add(Double.parseDouble(line))
            }
        }
        catch {
            case e: FileNotFoundException => {
                throw new ConfigurationException("Similarity threshold file '" + similarityThresholdsFile + "' not found in index directory " + contextIndexDirectory, e)
            }
            case e: NumberFormatException => {
                throw new ConfigurationException("Error parsing similarity value in '" + contextIndexDirectory + "/" + similarityThresholdsFile, e)
            }
            case e: IOException => {
                throw new ConfigurationException("Error reading '" + contextIndexDirectory + "/" + similarityThresholdsFile, e)
            }
        }
        taggerFile = config.getProperty("org.dbpedia.spotlight.tagging.hmm", "").trim
        if (taggerFile == null || !new File(taggerFile).isFile) {
            throw new ConfigurationException("Cannot find POS tagger model file " + taggerFile)
        }
        language = config.getProperty("org.dbpedia.spotlight.language", "English")
        stopWordsFile = config.getProperty("org.dbpedia.spotlight.data.stopWords." + language.toLowerCase, "").trim
        if ((stopWordsFile == null) || !new File(stopWordsFile.trim).isFile) {
            LOG.warn("Cannot find stopwords file '" + stopWordsFile + "'. Using default Lucene Analyzer StopWords.")
        }
        else {
            try {
                val bufferedReader: BufferedReader = new BufferedReader(new FileReader(stopWordsFile.trim))
                var line: String = null
                stopWords = new HashSet[String]
                while ((({
                    line = bufferedReader.readLine; line
                })) != null) {
                    stopWords.add(line.trim)
                }
                bufferedReader.close
            }
            catch {
                case e1: Exception => {
                    LOG.error("Could not read stopwords file. Using default Lucene Analyzer StopWords")
                }
            }
        }
        analyzer = Factory.analyzer.from(config.getProperty("org.dbpedia.spotlight.lucene.analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer"), config.getProperty("org.dbpedia.spotlight.lucene.version", "LUCENE_36"), stopWords)
        serverURI = config.getProperty("org.dbpedia.spotlight.web.rest.uri", "").trim
        if (serverURI != null && !serverURI.endsWith("/")) {
            serverURI = serverURI.concat("/")
        }
        try {
            new URI(serverURI)
        }
        catch {
            case e: URISyntaxException => {
                throw new ConfigurationException("Server URI not valid.", e)
            }
        }
        BooleanQuery.setMaxClauseCount(3072)
        sparqlEndpoint = config.getProperty("org.dbpedia.spotlight.sparql.endpoint", "").trim
        sparqlMainGraph = config.getProperty("org.dbpedia.spotlight.sparql.graph", "").trim
        val maxCacheSizeString: String = config.getProperty("jcs.default.cacheattributes.MaxObjects", "").trim
        try {
            maxCacheSize = new Long(maxCacheSizeString.trim)
        }
        catch {
            case ignored: Exception => {
                LOG.error(ignored)
            }
        }
        val coreDbType: String = config.getProperty("org.dbpedia.spotlight.core.database", "").trim
        val coreJdbcDriver: String = config.getProperty("org.dbpedia.spotlight.core.database.jdbcdriver", "").trim
        val coreDbConnector: String = config.getProperty("org.dbpedia.spotlight.core.database.connector", "").trim
        val coreDbUser: String = config.getProperty("org.dbpedia.spotlight.core.database.user", "").trim
        val coreDbPassword: String = config.getProperty("org.dbpedia.spotlight.core.database.password", "").trim
        try {
            if (coreDbType == "jdbc") {
                LOG.info("Core database from JDBC: " + coreDbConnector)
                dbpediaResourceFactory = new DBpediaResourceFactorySQL(coreJdbcDriver, coreDbConnector, coreDbUser, coreDbPassword)
            }
            else {
                LOG.info("Core database from Lucene: " + contextIndexDirectory)
            }
        }
        catch {
            case e: Exception => {
                LOG.warn("Tried to use core database provided, but failed. Will use Lucene index as core database.", e)
            }
        }

        SpotlightConfiguration()
    }

    private def getProp(config: Properties, property: String, deprecatedProperty: String, defaultVal: String): String = {
        val prop: String = config.getProperty(property)
        val deprProp: String = config.getProperty(deprecatedProperty)
        if (deprProp != null) {
            var msg: String = "property " + deprecatedProperty + " is deprecated"
            if (prop != null) {
                msg += "; is overwritten by " + property
            }
            else {
                msg += "; please use " + property
            }
            LOG.warn(msg)
        }
        if (prop != null) {
            return prop
        }
        else if (deprProp != null) {
            return deprProp
        }
        else {
            return defaultVal
        }
    }




    final val DEFAULT_TEXT: String = ""
    final val DEFAULT_URL: String = ""
    final val DEFAULT_CONFIDENCE: String = "0.1"
    final val DEFAULT_SUPPORT: String = "10"
    final val DEFAULT_TYPES: String = ""
    final val DEFAULT_SPARQL: String = ""
    final val DEFAULT_POLICY: String = "whitelist"
    final val DEFAULT_COREFERENCE_RESOLUTION: String = "true"
    @Deprecated var DEFAULT_RESOURCE_NAMESPACE: String = "http://dbpedia.org/resource/"
    @Deprecated var DEFAULT_ONTOLOGY_NAMESPACE: String = "http://dbpedia.org/ontology/"
    @Deprecated var DEFAULT_LANGUAGE_I18N_CODE: String = "en"
    @Deprecated final val DEFAULT_STOPWORDS = Set("a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with")


    def toString = "SpotlightConfiguration[configFileName=%s]".format(configFileName)

    def getDBpediaResourceNamespace: String = {
        return dbpediaResourceNamespace
    }

    def getDBpediaOntologyNamespace: String = {
        return dbpediaOntologyNamespace
    }

    def getLanguage: String = {
        return language
    }

    def getI18nLanguageCode: String = {
        return i18nLanguageCode
    }

    def isContextIndexInMemory: Boolean = {
        return disambiguatorConfiguration.isContextIndexInMemory
    }

    def isCandidateMapInMemory: Boolean = {
        return candidateMapInMemory
    }

    def getServerURI: String = {
        return serverURI
    }

    def getContextIndexDirectory: String = {
        return disambiguatorConfiguration.getContextIndexDirectory
    }

    def getCandidateIndexDirectory: String = {
        return candidateMapDirectory
    }

    def getSimilarityThresholds: List[Double] = {
        return similarityThresholds
    }

    def getSparqlMainGraph: String = {
        return sparqlMainGraph
    }

    def getSparqlEndpoint: String = {
        return sparqlEndpoint
    }

    def getTaggerFile: String = {
        return taggerFile
    }

    def getStopWords: Set[String] = {
        return stopWords
    }

    def getMaxCacheSize: Long = {
        return maxCacheSize
    }

    def getDBpediaResourceFactory: DBpediaResourceFactory = {
        return dbpediaResourceFactory
    }

    def getSpotterConfiguration: SpotterConfiguration = {
        return spotterConfiguration
    }

    def getDisambiguatorConfiguration: DisambiguatorConfiguration = {
        return disambiguatorConfiguration
    }

    def getAnalyzer: Analyzer = {
        return analyzer
    }



}