from elasticsearch import helpers, Elasticsearch
import csv

es = Elasticsearch()

with open('SpeedDatingData.csv') as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='dating', doc_type='date')