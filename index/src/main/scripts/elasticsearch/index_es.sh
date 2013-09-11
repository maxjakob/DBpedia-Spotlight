# DBpedia Spotlight's indexing procedure using ElasticSearch
# ====================
# !! IN DEVELOPMENT !!
# ====================

ES_SERVER=$1
ES_SETTINGS_JSON=$2
INDEX_NAME=$3
WORKING_DIR=$4

#TODO check parameters

set -e

# transform to bulk JSONs
for freqsFile in tokenCounts pairCounts uriCounts; do
    echo "pig2es.py $WORKING_DIR/$freqsFile"
    ./pig2es.py $INDEX_NAME $WORKING_DIR/$freqsFile
done
#TODO for DBpedia types
#TODO index sfAndTotalCounts also in ES?


# create index
# TODO be careful with deleting
echo "deleting index $ES_SERVER/$INDEX_NAME\n"
curl -XDELETE $ES_SERVER/$INDEX_NAME
echo "\ncreating index $ES_SERVER/$INDEX_NAME"
curl -XPOST $ES_SERVER/$INDEX_NAME --data-binary @$ES_SETTINGS_JSON
echo


# do bulk index
for jsonFile in $WORKING_DIR/*.json; do
    echo "indexing $jsonFile ..."
    curl -XPOST $ES_SERVER/_bulk --data-binary @$jsonFile \
        --silent --show-error > /dev/null
done

