from datetime import datetime
from elasticsearch import Elasticsearch
import argparse, gzip, boto3

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--numdays', help='Number of days back from today to archive, default is 0', default=0)
    parser.add_argument('--archive', help='Archive name')
    parser.add_argument('--esaddress', help='Elasticsearch Address', default='localhost:9200')
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--dry', help='Dry run', action='store_true')
    args = parser.parse_args()

    numDays = args.numdays
    archiveName = args.archive
    now = datetime.now()
    indexDay = int(now.day) - int(numDays)
    indexName = 'logstash-%s.%02d.%s' % (now.year, now.month, indexDay)
    stuffs = []

    es = Elasticsearch(hosts=args.esaddress)

    query={"query" : {"match_all" : {}}}

    if es.search_exists(index=indexName):
        rs = es.search(index=indexName, scroll='60s', search_type='scan', size=100, body=query)
        scroll_size = rs['hits']['total']
        while (scroll_size > 0):
            try:
                scroll_id = rs['_scroll_id']
                rs = es.scroll(scroll_id=scroll_id, scroll='60s')
                stuffs += rs['hits']['hits']
                scroll_size = len(rs['hits']['hits'])
            except:
                break
    else:
        print 'Index %s does not exist' % indexName
        exit()

    with gzip.open(archiveName + '.gz', 'wb') as f:
        for stuff in stuffs:
            f.write(str(stuff))
        f.close

    if args.dry:
        print "Not deleting index %s" % indexName
    else:
        es.indices.delete(index=indexName)

    print 'Pushing to bucket name %s' % args.bucket
    s3 = boto3.resource('s3')
    data = open(archiveName + '.gz', 'rb')
    s3.Bucket(args.bucket).put_object(Key=archiveName + '.gz', Body=data)

if __name__ == "__main__":
    main()