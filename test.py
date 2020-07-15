import cdx_toolkit
import gzip
import json
import requests
import trafilatura
from lxml import html
import logging
import time
from io import BytesIO

cdx = cdx_toolkit.CDXFetcher(source='cc')
url = 'dr.dk/nyheder/indland/*'

def extract_from_html(html_content):
    mytree = html.fromstring(html_content)
    article = trafilatura.extract(mytree, with_metadata=True, json_output=True)
    if article is None:
        return None
    return json.loads(article)

def extract(filename, offset, length, **trash):
    try:
        offset, length = int(offset), int(length)
        offset_end = offset + length - 1
        prefix = 'https://commoncrawl.s3.amazonaws.com/'
        resp = requests.get(prefix + filename, headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
       
        raw_data = BytesIO(resp.content)
        f = gzip.GzipFile(fileobj=raw_data)

        data = f.read().decode()
        data = data.strip().split('\r\n\r\n')
        if len(data) != 3:
            logging.error("no response in warc")
            return
        warc, header, response = data
        article = extract_from_html(response)
        if not article:
            logging.error("no article")
            return
        print(json.dumps(article), flush=True)
    except KeyboardInterrupt:
        raise
    except:
        logging.exception('error')
        time.sleep(5)
for index, obj in enumerate(cdx.iter(url, filter=["status:200"])):
    logging.warning(f"downloading #{index}: {obj.data['url']}")
    extract(**obj.data)
