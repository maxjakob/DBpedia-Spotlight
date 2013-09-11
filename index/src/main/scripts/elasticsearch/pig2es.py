#!/usr/bin/python

"""
Prototypical transformation script for pignlproc output to ElasticSearch
bulk input JSONs. This should probably be transformed into a Pig UDF or
at least a Scala script, so that WikipediaToDBpediaClosure can be used.
In the Pig case, there is the possibility of joining "tokenCounts",
"pairCounts" and "uriCounts" together and produce even more compact
bulk indexing operations with multiple fields.

See also: ### Bulk import doc page.
The _id of the documents is the URI.

For now, this does not include custom routing. The initial idea was to
spread the documents in a way, that is is unlikely for two concepts that
share a surface form to end up in the same shard. This might have the
advantage that work can be parallalized and work in the individual shards
is minimized. However, it would complicate the indexing process, since
we need to keep a mapping of surface forms to all shards that they have
been sent to. ElasticSearch uses the has of the ID plus a random sequence
to determine the shard, which should already give a good distribution.
"""

import sys
import os.path


BULK_SIZE = 5000

TYPE_NAME = "entities"
SURFACE_FORM = "sf"
CONTEXT_FIELD = "context"
URI_COUNT = "uriCount"


def onlyUriEnding(wikiUrl):
    # TODO use WikipediaToDBpediaClosure.scala
    return wikiUrl[wikiUrl.index("/wiki/")+6:]


def parseTokenCounts(line):
    def toTuple(s):
        i = s.rindex(",")
        return (s[:i], s[i+1:])

    wikiUrl, data = line.strip().split("\t")
    uri = onlyUriEnding(wikiUrl)
    if data == '{}':
        freqs = []
    else:
        stripped = data[2:-2]  # remove starting '{(' and ending ')}'
        freqs = map(toTuple, stripped.split("),("))
    context = " ".join(map(lambda (t, f): (t+" ")*int(f), freqs))
    return uri, CONTEXT_FIELD, '"'+context+'"'


def parsePairCounts(line):
    sf, wikiUrl, freq = line.strip().split("\t")
    uri = onlyUriEnding(wikiUrl)
    return uri, SURFACE_FORM, '"'+sf+'"'


def parseUriCounts(line):
    wikiUrl, freq = line.strip().split("\t")
    uri = onlyUriEnding(wikiUrl)
    return uri, URI_COUNT, freq


def parseSfCounts(line):
    raise NotImplementedError, "not implemented"  # because not indexed


lineParsers = {
    "tokenCounts": parseTokenCounts,
    "pairCounts": parsePairCounts,
    "uriCounts": parseUriCounts,
    "sfAndTotalCounts": parseSfCounts
}


def parseFile(fileName):
    parser = lineParsers[os.path.basename(fileName)]
    with open(fileName) as f:
        for line in f:
            yield parser(line)


def toJson(indexName, typeName, uri, fieldName, val):
    indexLine = '{"index":{"_index":"%s","_type":"%s","_id":"%s"}}' % (
        indexName, typeName, uri)
    fieldsLine = '{"%s":%s}' %  (fieldName, val)
    return indexLine + "\n" + fieldsLine + "\n"


def newFile(fileNameBase, i):
    return open("%s_%04d.json" % (fileNameBase, i), "w")


if __name__ == '__main__':
    indexName, pigOutputFileName = sys.argv[1:3]

    bulks = 1
    outFile = newFile(pigOutputFileName, bulks)

    for idx, (uri, field, val) in enumerate(parseFile(pigOutputFileName)):
        json = toJson(indexName, TYPE_NAME, uri, field, val)
        outFile.write(json)

        if (idx+1)%BULK_SIZE == 0:
            bulks += 1
            outFile.close()
            outFile = newFile(pigOutputFileName, bulks)
    outFile.close()

