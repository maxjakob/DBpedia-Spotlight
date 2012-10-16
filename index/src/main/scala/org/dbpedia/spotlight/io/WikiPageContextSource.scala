package org.dbpedia.spotlight.io

/**
 * Copyright 2011 Pablo Mendes, Max Jakob
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

import org.dbpedia.extraction.sources.Source
import org.dbpedia.spotlight.string.WikiMarkupStripper
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.spotlight.model._
import org.dbpedia.extraction.wikiparser.TextNode
import org.dbpedia.extraction.wikiparser.InternalLinkNode


object WikiPageContextSource extends WikipediaOccurrenceSourceFactory {

    protected def occurrenceSource(extractionSource: Source) = new WikiPageContextSource(extractionSource)

    def getPageText(node : Node) : String = {
        node.children.map{
            _ match
            {
                case textNode : TextNode => WikiMarkupStripper.stripMultiPipe(textNode.text.trim)
                case internalLink : InternalLinkNode => { getPageText(internalLink) }
                case _ => ""
            }
        }.mkString(" ").replaceAll("""\n""", " ").replaceAll("""\s""", " ")
    }
}

/**
 * DBpediaResourceOccurrence Source which reads from a wiki pages source.
 */
class WikiPageContextSource(wikiPages : Source) extends OccurrenceSource {
    val wikiParser = WikiParser()

    override def foreach[U](f : DBpediaResourceOccurrence  => U) {
        for (wikiPage <- wikiPages) {
            // clean the wiki markup from everything but links
            val cleanSource = WikiMarkupStripper.stripEverything(wikiPage.source)

            // parse the (clean) wiki page
            val pageNode = wikiParser( WikiPageUtil.copyWikiPage(wikiPage, cleanSource) )

            // exclude redirects, disambiguation pages
            if (!pageNode.isRedirect && !pageNode.isDisambiguation) {
                val pageContext = new Text( WikiPageContextSource.getPageText(pageNode) )
                val resource = new DBpediaResource(pageNode.title.encoded)
                val surfaceForm = Factory.SurfaceForm.fromWikiPageTitle(pageNode.title.decoded, false)
                f( new DBpediaResourceOccurrence(pageNode.id.toString, resource, surfaceForm, pageContext, -1) )
            }
        }
    }
}



