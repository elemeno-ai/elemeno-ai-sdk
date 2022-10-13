
import asyncio
import io
import logging
import time
from typing import Dict, List, Optional
import aiohttp
from distributed import as_completed
import requests
from elemeno_ai_sdk.cos.minio import MinioClient
from elasticsearch import Elasticsearch
from io import BytesIO, SEEK_SET, SEEK_END
from dask.distributed import Client

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

def install():
  import os
  import sys
  os.system(f"{sys.executable} -m pip install elemeno-ai-sdk==0.2.14 minio")

def get_and_upload_dask(to_get: Dict) -> None:
    from elemeno_ai_sdk.cos.minio import MinioClient
    client = MinioClient(host="minio.minio:9000",
            access_key="elemeno",
            secret_key="minio123",
            use_ssl=False)
    try:
        #logging.error("Starting")
        media_id = to_get['id']
        media_url = to_get['url'].replace("\\", "")
        offer_id = to_get['offer_id']
        position = to_get['position']
        headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
        # fixes for bel description case
        #logging.error(media_url)
        media_url = media_url.replace('/{description}.', "/x.")
        r = requests.get(media_url, headers=headers, stream=True)
        content_type = r.headers['content-type']
        if content_type.startswith('image'):
            st = io.BytesIO(r.content)
            media_extension = media_url.split('.')[-1]
            try:
                #logging.error("Will try to list")
                #existing = client.list_dir('elemeno-cos', f"binary_data_parallel/{offer_id}/")
                #logging.error("After list")
                #if len(list(existing)) == 0:
                #    logging.error("is 0")
                raise ValueError("list_dir didn't throw exception but content was 0")
                #logging.error(f"Listing {offer_id} already exists")
                #print("Will skip offer_id: " + offer_id)
            except Exception as e:
                logging.error("will upload offer_id: " + offer_id)
                #print(e)
                client.put_object('elemeno-cos', f"binary_data_parallel/{offer_id}/{position}_{media_id}.{media_extension}", st)
                logging.error("uploaded path: " + f"binary_data_parallel/{offer_id}/{position}_{media_id}.{media_extension}")
                #logging.error(f'{media_id}: {media_url}')
                return True
        else:
          logging.error(f'{media_id}: {media_url}')
          logging.error("Not an image")
          return False
    except Exception as e:
        logging.error(f"Failed reading file URL {to_get['url']}")
        logging.error(e)
        return False

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
        dask_client = Client(address="a6304c020a60a460cb97247a70f88d6f-805997406.us-east-1.elb.amazonaws.com:8786", timeout=300)
        print(dask_client.run(install))
        dask_client.close()
        print("after install")
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
            logging.info("Total number of files: " + str(total))
            pages = total // 100 + 1
            logging.info(f'pages: {pages}')
            futures = []
            for page in range(1, pages):
              dask_client = Client(address="a6304c020a60a460cb97247a70f88d6f-805997406.us-east-1.elb.amazonaws.com:8786", timeout=300)
              logging.debug(f'page: {page}')
              res = es.search(index=index_name, body=query, size=100, from_=page*100)
              assets_all = self.prepare_medias(res['hits']['hits'])
              logging.info("Size of assets_all: " + str(len(assets_all)))
              try:
                #await self.get_assets_in_parallel(assets_dist)
                try:
                  logging.info("Will send work for dask")
                  logging.info("len(assets): " + str(len(assets_all)))
                  futures.extend(dask_client.map(get_and_upload_dask, assets_all, batch_size=200))
                except Exception as e:
                  logging.error("Failed on dask submit")
                  logging.error(e)
              except:
                logging.error("Failed getting file URL DASK")
                pass
              if (page % 50) == 0:
                logging.info("Will wait on futures to complete")
                for f in as_completed(futures):
                  try:
                    f.result()
                    print("Finished")
                  except Exception as e:
                    logging.error(f"Failed getting file URL")
                    logging.error(e, stack_info=True)
                    pass
                logging.info(f'pages up to {page} done')
                futures = []
                dask_client.close()
          
        logging.info("Finished processing all entries")
        pass

    def prepare_medias(self, properties: List[Dict]) -> List:
        for p in properties:
            for i, m in enumerate(p['_source']['media']):
                m['position'] = i
        # sort the properties by number of assets in descending order
        sorted_properties = sorted(properties, key=lambda x: len(x['_source']['media']), reverse=True)

        # create a list of lists to hold the resulting array
        result = []

        # iterate over the properties
        for property in sorted_properties:
            for asset in property['_source']['media']:
                asset['offer_id'] = property['_source']['id']
                result.append(asset)

        return result

    async def get_assets_in_parallel(self, assets: List[List]) -> None:
        import aiohttp
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            coroutines = [self.get_and_upload(session, asset) for asset in assets]
            await asyncio.gather(*coroutines)
    
    
    async def get_and_upload(self, session: aiohttp.ClientSession, to_get: List[Dict]) -> None:
        for asset in to_get:
            try:
                #await self.get_image_and_upload(session, asset)
                pass
            except Exception as e:
                logging.error(f"Failed reading file URL {asset['url']}")
                logging.error(e)
                pass

    # async def get_image_and_upload(self, session: aiohttp.ClientSession, media: Dict) -> None:
    #     """
    #     This method is used to get an image from Elasticsearch and upload it to COS.
    #     """
    #     media_id = media['id']
    #     media_url = media['url'].replace("\\", "")
    #     offer_id = media['offer_id']
    #     position = media['position']
    #     headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
    #     # fixes for bel description case
    #     media_url = media_url.replace('/{description}.', "/x.")
    #     async with session.get(media_url, headers=headers, timeout=5, stream=True) as r:
    #     #r = requests.get(media_url, stream=True, verify=False, headers=headers, timeout=5)
    #         content_type = r.headers['content-type']
    #         if content_type.startswith('image'):
    #             st = io.BytesIO(await r.content)
    #             media_extension = media_url.split('.')[-1]
    #             try:
    #                 existing = self._client.list_dir('elemeno-cos', f"binary_data_parallel/{offer_id}/")
    #                 if len(list(existing)) == 0:
    #                     raise ValueError("list_dir didn't throw exception but content was 0")
    #                 #print("Will skip offer_id: " + offer_id)
    #             except Exception as e:
    #                 logging.error("will upload offer_id: " + offer_id)
    #                 #print(e)
    #                 self._client.put_object('elemeno-cos', f"binary_data_parallel/{offer_id}/{position}_{media_id}.{media_extension}", st)
    #                 logging.debug(f'{media_id}: {media_url}')
    #         else:
    #             logging.error(f"Failed reading file URL {media_url}")
    #             pass
    #     pass


if __name__ == '__main__':

    # sets log level to info
    # logging.basicConfig(filename="out.log",
    #                 filemode='w+',
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
    # for f in client.list_dir('elemeno-cos', 'binary_data/'):
    #     print(f.object_name)
    #self.dask_client.run(install)
    es_to_cos = ElasticToCos(None)
    # get current loop or create one
    #loop = asyncio.get_event_loop()
    #task = asyncio.create_task(es_to_cos.query_file_urls("listing-br-rj-rio.de.janeiro"))
    async def main():
      qfilter = { "query": { "bool" : { "must_not" : { "term" : { "source.name" : "Loft" } } } } }
      await es_to_cos.query_file_urls("listing-br-rj-rio.de.janeiro", qfilter)
    asyncio.run(main())
    #asyncio.ensure_future(task)
 