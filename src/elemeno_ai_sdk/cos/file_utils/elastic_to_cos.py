
import logging
import time
from typing import Dict, Optional
import requests
from requests import session
from elemeno_ai_sdk.cos.minio import MinioClient
from elasticsearch import Elasticsearch
from io import BytesIO, SEEK_SET, SEEK_END

class ResponseStream(object):
    def __init__(self, request_iterator):
        self._bytes = BytesIO()
        self._iterator = request_iterator

    def _load_all(self):
        self._bytes.seek(0, SEEK_END)
        for chunk in self._iterator:
            self._bytes.write(chunk)

    def _load_until(self, goal_position):
        current_position = self._bytes.seek(0, SEEK_END)
        while current_position < goal_position:
            try:
                current_position += self._bytes.write(next(self._iterator))
            except StopIteration:
                break

    def tell(self):
        return self._bytes.tell()

    def read(self, size=None):
        left_off_at = self._bytes.tell()
        if size is None:
            self._load_all()
        else:
            goal_position = left_off_at + size
            self._load_until(goal_position)

        self._bytes.seek(left_off_at)
        return self._bytes.read(size)
    
    def seek(self, position, whence=SEEK_SET):
        if whence == SEEK_END:
            self._load_all()
        else:
            self._bytes.seek(position, whence)

class ElasticToCos:

    def __init__(self, client: MinioClient) -> None:
        """
        This class is used to copy files from Elasticsearch to COS.
        """
        from elemeno_ai_sdk.config import Configs
        cfg = Configs.instance()
        self._client = client
        elastic_cfg = cfg.feature_store.additional.elastic
        self._es = Elasticsearch(hosts=[elastic_cfg.host],
            http_auth=(elastic_cfg.user, elastic_cfg.password))
        pass
    
    async def query_file_urls(self, index_name: str, query: Optional[Dict] = None) -> list:
        """
        This method is used to query files from Elasticsearch.
        """
        es = self._es
        count = es.count(index=index_name)
        logging.info(f'count: {count}')
        if 'count' in count:
            total = count['count']
            pages = total // 1000 + 1
            logging.info(f'pages: {pages}')
            for page in range(1, pages):
                logging.info(f'page: {page}')
                res = es.search(index=index_name, body=query, size=100, from_=page*100)
                for hit in res['hits']['hits']:
                    for media in hit['_source']['media']:
                        try:
                            await self.get_image_and_upload(media)
                        except Exception as e:
                            logging.error(f"Failed reading file URL {media['url']}")
                            logging.error(e)
                            pass
        logging.info("Finished processing all entries")
        pass

    async def get_image_and_upload(self, media: Dict) -> None:
        """
        This method is used to get an image from Elasticsearch and upload it to COS.
        """
        media_id = media['id']
        media_url = media['url']
        headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
        async with session.get(media_url, stream=True, verify=False, headers=headers, timeout=5) as r:
        #r = requests.get(media_url, stream=True, verify=False, headers=headers, timeout=5)
            content_type = r.headers['content-type']
            if content_type.startswith('image'):
                stream = ResponseStream(r.iter_content(64))
                stream_content = stream.read()
                stream_size = len(stream_content)
                #size = sum(len(chunk) for chunk in r.iter_content(8196))
                stream.seek(0)
                media_extension = media_url.split('.')[-1]
                self._client.put_object('elemeno-cos', f"binary_data/{media_id}.{media_extension}", stream, stream_size)
                logging.debug(f'{media_id}: {media_url}')
                time.sleep(0.5)
            else:
                logging.error(f"Failed reading file URL {media_url}")
                pass
        pass

if __name__ == '__main__':

    client = MinioClient()
    # for f in client.list_dir('elemeno-cos', 'binary_data/'):
    #     print(f.object_name)
    es_to_cos = ElasticToCos(client)
    es_to_cos.query_file_urls("listing-br-rj-rio.de.janeiro-copacabana")