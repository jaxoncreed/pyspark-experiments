FILE_URL = "https://www.kaggle.com/annavictoria/speed-dating-experiment/downloads/Speed%20Dating%20Data.csv.zip"
FILE_NAME = "./SpeedDatingData.csv"
ES_HOST = {"host" : "159.203.184.137", "port" : 9200}
INDEX_NAME = 'dating'
TYPE_NAME = 'date'
ID_FIELD = 'iid'


import csv
import urllib2
import codecs
import math
# response = urllib2.urlopen(FILE_URL)
response = codecs.open(FILE_NAME, "rU", encoding='UTF8')
csv_file_object = csv.reader(response)

header = csv_file_object.next()
header = [item.lower() for item in header]

bulk_data = []
index = 0
for row in csv_file_object:
    data_dict = {}
    for i in range(len(row)):
        data_dict[header[i]] = row[i]
    op_dict = {
        "index": {
            "_index": INDEX_NAME,
            "_type": TYPE_NAME,
            "_id": data_dict[ID_FIELD]
        }
    }
    partitionNumber = int(math.floor(index / 500))
    if len(bulk_data) - 1 < partitionNumber:
        bulk_data.append([])
    bulk_data[partitionNumber].append(op_dict)
    bulk_data[partitionNumber].append(data_dict)
    index += 1

print(len(bulk_data[0]))
print(len(bulk_data))

from elasticsearch import Elasticsearch
# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST])
if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))
# since we are running locally, use one shard and no replicas
request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}
print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print(" response: '%s'" % (res))

for bulk_partition in bulk_data:
    # bulk index the data
    print("bulk indexing...")
    res = es.bulk(index = INDEX_NAME, body = bulk_partition, refresh = True)

    # sanity check
    res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
    print(" response: '%s'" % (res))