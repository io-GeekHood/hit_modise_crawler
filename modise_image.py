from minio import Minio
import asyncio
import io
import logging
import os
import os.path
import aiohttp
import pandas as pd
from aiohttp_proxy import ProxyConnector
import requests


MinioHost = os.environ.get('AWS_HOST', 'localhost:9000')
S3Host = os.environ.get('S3_HOST', 'http://localhost:9000/')
MinioUser = os.environ.get('AWS_ACCESS', 'minioadmin')
MinioPass = os.environ.get('AWS_SECRET', 'sghllkfij,dhvrndld')
IMAGE_BUCKET = os.environ.get('MDS_IMG_BUCK_NAME',"modise-images-main")
REFRENCE_BUCKET = os.environ.get('MDS_REF_BUCK_NAME',"modise-refrence-main")



# try:
#     initiator =  Minio(
#             MinioHost,
#             access_key=MinioUser,
#             secret_key=MinioPass,
#             secure=False
#         )
#     img_found = initiator.bucket_exists(IMAGE_BUCKET)
#     if not img_found:
#         initiator.make_bucket(IMAGE_BUCKET)
#     else:
#         logging.warning(f"Image-Bucket {IMAGE_BUCKET} already exists")
#     ref_found = initiator.bucket_exists(REFRENCE_BUCKET)
#     if not ref_found:
#         initiator.make_bucket(REFRENCE_BUCKET)
#     else:
#         logging.warning(f"Refrence-Bucket {REFRENCE_BUCKET} already exists")
# except Exception as fail:
#     logging.warning(f"Unable to Access S3 filesystem at: {MinioHost} \n access_key : {MinioUser} \n secret_key : {MinioPass}")
#     pass

def check_object_exists(client,bucket,filename):
    logging.info(f"searching for {filename} in {bucket}")
    try:
        objstat = client.stat_object(bucket,filename)
        minio_obj = objstat.object_name
        if filename == minio_obj:
            logging.info(f"{filename} found !")
            return True
        else:
            logging.info(f"{filename} not found!")
            return False
    except Exception as fail:
        logging.info(f"{filename} does not exist in bucket (fetching ...)")
        return False
def minio_parquet_upload(idx:int,datas:pd.DataFrame):
    try:
        client = Minio(
            MinioHost,
            access_key=MinioUser,
            secret_key=MinioPass,
            secure=False
        )
    except Exception as fail:
        logging.error(f"failed to create connection to minio address {MinioHost} user = {MinioUser} pass = {MinioPass} |\n {fail}")

    try:
        datas["file_id"] = datas["file_id"].astype(str)
        local_path = f"refrences/checkpoint_{idx}.parquet"
        datas.to_parquet(local_path)
        client.fput_object(REFRENCE_BUCKET, f"checkpoint_{idx}.parquet",local_path)
        logging.info(f"{local_path} is successfully uploaded archive {REFRENCE_BUCKET} (removing temp file)")
    except Exception as fail:
        logging.error(f"failed to insert parquet file on minio filesystem| {fail}")
    finally:
        os.remove(local_path)

def get_new_proxy_media():
    PRX = os.getenv('PROXY_PROVIDER', 'http://127.0.0.1:8000/proxy')
    try:
        result = requests.get(PRX)
        return result.json()
    except:
        logging.error(f"failed to get new proxy from {PRX}")
        return {}

async def minio_image_uploader(filename:str,datas:object,lenght:int):

    client = Minio(
        MinioHost,
        access_key=MinioUser,
        secret_key=MinioPass,
        secure=False
    )

    client.put_object(bucket_name=IMAGE_BUCKET,object_name=filename,length=lenght,data=datas)


async def get_page_media(session, job):
    logging.info(f"fetch media job {job}")
    try:
        url = job["url"]
        id = job["file_id"]
        idx = job["file_index"]
        full_name = f"{id}_{idx}.jpg"

        async with session.get(url) as result:
            try:
                logging.info(f"get request on : {url} name {full_name}")
                if result.status == 200:
                    batch = await result.read()
                    b = bytearray(batch)
                    byte_len = len(b)
                    value_as_a_stream = io.BytesIO(b)
                    return await minio_image_uploader(full_name, value_as_a_stream, byte_len)
            except Exception as fail:
                logging.error(f"Failed to get image {fail}")
                return
    except Exception as fail:
        logging.info(f"fetch media job {job}")
        logging.error(f"failed with session.get (media) request ! {fail}")
        return


async def register_page_media(session, jobs):
    try:
        tasks = []
        for url in jobs:
            task = asyncio.create_task(get_page_media(session, url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results
    except Exception as fail:
        logging.error(f"failed with page main registeration task pool function on media downloader {fail}")
        return ""


async def fetch_media(jobs):
    logging.info(f"fetch media job {jobs}")
    IsLocal = os.environ.get('LOCAL', 'true')
    if bool(IsLocal):
        connector = aiohttp.connector.TCPConnector(limit=25, limit_per_host=25)
    else:
        new_proxy = get_new_proxy_media()
        connector = ProxyConnector.from_url(new_proxy["uri"])
    custom_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0)"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, headers=custom_headers) as session:
            data = await register_page_media(session, jobs)
            return data
    except Exception as fail:
        logging.error(f"failed with page registeration asyn call on media downloader! {fail}")

if __name__ == "__main__":
    pass
    # test proxy fetch job
    # proxies = get_new_proxy()
    # logging.debug(f"new proxy = {proxies}")

    # test image download job
    # stats = [{
    #     "product": "myprod",
    #     "file_id": "1234",
    #     "file_index": 3,
    #     "checkpoint_file": "10",
    #     "url": "https://media.sproutsocial.com/uploads/2017/02/10x-featured-social-media-image-size.png"
    # }]
    # asyncio.run(fetch_media(stats))