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
echo "making bulk JSONs for uriCounts"
./pig2es.py $INDEX_NAME $WORKING_DIR/uriCounts index
echo "making bulk JSONs for pairCounts"
./pig2es.py $INDEX_NAME $WORKING_DIR/pairCounts update
echo "making bulk JSONs for tokenCounts"
./pig2es.py $INDEX_NAME $WORKING_DIR/tokenCounts update
#TODO for DBpedia types
#TODO index sfAndTotalCounts also in ES?


# create index
# TODO be careful with deleting
echo "deleting index $ES_SERVER/$INDEX_NAME"
curl -XDELETE $ES_SERVER/$INDEX_NAME
echo
echo "creating index $ES_SERVER/$INDEX_NAME"
curl -XPOST $ES_SERVER/$INDEX_NAME --data-binary @$ES_SETTINGS_JSON
echo


# do bulk index
for jsonFile in \
    $WORKING_DIR/uriCounts*.json \
    $WORKING_DIR/pairCounts*.json \
    $WORKING_DIR/tokenCounts*.json; do
    echo "indexing $jsonFile ..."
    curl -XPOST $ES_SERVER/_bulk --data-binary @$jsonFile \
        --silent --show-error  > /dev/null
done

