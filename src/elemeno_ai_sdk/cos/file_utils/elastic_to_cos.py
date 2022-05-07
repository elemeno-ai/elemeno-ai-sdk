
import asyncio
import io
import logging
import time
from typing import Dict, List, Optional
import aiohttp
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
        elastic_cfg = cfg.feature_store.binary_source.elastic
        self._es = Elasticsearch(hosts=[elastic_cfg.host],
            http_auth=(elastic_cfg.user, elastic_cfg.password))
        pass
    
    async def query_file_urls(self, index_name: str, query: Optional[Dict] = None) -> list:
        """
        This method is used to query files from Elasticsearch.
        """
        es = self._es
        count = es.count(index=index_name)
        logging.debug(f'count: {count}')
        if 'count' in count:
            total = count['count']
            pages = total // 1000 + 1
            logging.debug(f'pages: {pages}')
            for page in range(1, pages):
                logging.debug(f'page: {page}')
                res = es.search(index=index_name, body=query, size=100, from_=page*100)
                assets_dist = self.split_assets(res['hits']['hits'], 10)
                await self.get_assets_in_parallel(assets_dist)
                logging.debug(f'page {page} done')
        logging.info("Finished processing all entries")
        pass

    def split_assets(self, properties: List[Dict], resulting_arr_length: int) -> List:
        for p in properties:
            for i, m in enumerate(p['_source']['media']):
                m['position'] = i
        # sort the properties by number of assets in descending order
        sorted_properties = sorted(properties, key=lambda x: len(x['_source']['media']), reverse=True)

        # create a list of lists to hold the resulting array
        result = [[] for _ in range(resulting_arr_length)]

        # iterate over the properties and evenly split the child array assets
        for property in sorted_properties:
            for i, asset in enumerate(property['_source']['media']):
                asset['offer_id'] = property['_source']['id']
                result[i % resulting_arr_length].append(asset)

        return result

    async def get_assets_in_parallel(self, assets: List[List]) -> None:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            coroutines = [self.get_and_upload(session, asset) for asset in assets]
            await asyncio.gather(*coroutines)
    
    async def get_and_upload(self, session: aiohttp.ClientSession, to_get: List[Dict]) -> None:
        for asset in to_get:
            try:
                await self.get_image_and_upload(session, asset)
            except Exception as e:
                logging.error(f"Failed reading file URL {asset['url']}")
                logging.error(e)
                pass

    async def get_image_and_upload(self, session: aiohttp.ClientSession, media: Dict) -> None:
        """
        This method is used to get an image from Elasticsearch and upload it to COS.
        """
        media_id = media['id']
        media_url = media['url']
        offer_id = media['offer_id']
        position = media['position']
        headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
        async with session.get(media_url, headers=headers, timeout=5) as r:
        #r = requests.get(media_url, stream=True, verify=False, headers=headers, timeout=5)
            content_type = r.headers['content-type']
            if content_type.startswith('image'):
                st = io.BytesIO(await r.content.read())
                media_extension = media_url.split('.')[-1]
                self._client.put_object('elemeno-cos', f"binary_data_parallel/{offer_id}/{position}_{media_id}.{media_extension}", st)
                logging.debug(f'{media_id}: {media_url}')
            else:
                logging.error(f"Failed reading file URL {media_url}")
                pass
        pass