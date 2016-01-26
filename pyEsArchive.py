from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import argparse, gzip, boto3

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--numdays', help='Number of days back from today to archive, default is 0', default=0)
    parser.add_argument('--archive', help='Archive name')
    parser.add_argument('--logtype', help='Log type cwl/logstash', default='cwl')
    parser.add_argument('--esaddress', help='Elasticsearch Address', default='localhost')
    parser.add_argument('--esport', help='Elasticsearch Port', default=9200)
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--awsaccesskey', help='AWS Access Key')
    parser.add_argument('--awssecretkey', help='AWS Secret Key')
    parser.add_argument('--awsregion', help='AWS Region', default='us-east-1')
    parser.add_argument('--dry', help='Dry run', action='store_true')

    args = parser.parse_args()

    numDays = args.numdays
    archiveName = args.archive
    now = datetime.now()
    indexDay = int(now.day) - int(numDays)
    indexName = '%s-%s.%02d.%s' % (args.logtype, now.year, now.month, indexDay)
    stuffs = []

    awsauth = AWS4Auth(args.awsaccesskey, args.awssecretkey, args.awsregion, 'es')

    es = Elasticsearch(
        hosts=[{'host': args.esaddress, 'port': int(args.esport)}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    print(es.info())

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