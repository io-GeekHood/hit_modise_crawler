import os
import sys
import aiohttp
import asyncio
import logging
import time
from pymongo import MongoClient
from aiohttp_proxy import ProxyConnector, ProxyType
import requests
from pprint import pprint
logging.basicConfig(level=logging.DEBUG)


def mongo_insert(client:MongoClient,obj:list):
    db = os.getenv('MODISE_DB', 'modise-main')
    for data_object in obj:
        if data_object:
            if data_object["status"] == "error":
                logging.debug(f"product is inactive or had no data response : {data_object['msg']}")
                continue

            try:
                item_keys = list(data_object["detail"].keys())
                data_object["detail"]["_id"] = data_object["detail"]["product_id"] if "product_id" in item_keys else data_object["detail"]["price"]
                logging.debug(f"call result data has {item_keys}")
                r = client[db]["product"].insert_one(data_object["detail"])
                logging.info(f"---- inserted new product_core with id {r.inserted_id} ----")
                save_state(data_object["detail"]["_id"])
            except Exception as fail:
                logging.error(f"Failed on modise product insert phase :{fail}")
        else:
            logging.warning(f"request had no response  {data_object} \n")


def mongo_exist(client:MongoClient,id:int):
    db = os.getenv('MODISE_DB', 'modise-main')
    found = client[db]["product_core"].count_documents({"_id":id},limit=1)
    if found:
        return True
    else:
        return False

async def get_page(session,job):
    try:
        logging.debug(f"sending post on {job}")
        async with session.post(job['url'],json=job['body']) as result:
            return await result.json()
    except Exception as fail:
        logging.error(f"failed with session.post request ! {fail}")


async def register_page(session, jobs):
    try:
        tasks = []
        for job in jobs:
            task = asyncio.create_task(get_page(session, job))
            tasks.append(task)
        results = await asyncio.gather(*tasks)

        return results
    except Exception as fail:
        logging.error(f"failed with page main registeration task pool function {fail}")
        return ""

def get_new_proxy():
    PRX = os.getenv('PROXY_PROVIDER', 'http://127.0.0.1:8000/proxy')
    try:
        result = requests.get(PRX)
        return result.json()
    except:
        logging.error(f"failed to get new proxy from {PRX}")
        return {}
async def fetch(jobs):
    IsLocal = os.getenv('LOCAL',"true")
    if bool(IsLocal):
        connector = aiohttp.connector.TCPConnector(limit=25, limit_per_host=25)
    else:
        new_proxy = get_new_proxy()
        connector = ProxyConnector.from_url(new_proxy["uri"])
    custom_headers = {
        "x-requested-with": "XMLHttpRequest"
    }
    try:
        async with aiohttp.ClientSession(connector=connector,headers=custom_headers) as session:
            data = await register_page(session, jobs)
            return data
    except Exception as fail:
        logging.error(f"failed with page registeration asyn call ! {fail}")



def save_state(number):
    with open("state_storage/data_state.txt","w") as st:
        st.write(str(number))
def get_state():
    with open("state_storage/data_state.txt","r") as st:
        last_insert = st.read()
    return int(last_insert)
if __name__ == '__main__':
    # load_dotenv()
    MongoHost = os.getenv('MONGODB_URI',"mongodb://hit_admin:*5up3r53CUR3D@127.0.0.1:27017")
    SLEEPING = os.getenv('SLEEPING',"10")
    logging.info(f"connecting to {MongoHost}")
    last_state = get_state()
    mongo_client = MongoClient(MongoHost)
    jobs_buffer = []
    for prod_number in range(last_state,1,-1):
        exist = mongo_exist(mongo_client,prod_number)
        if exist:
            logging.info(f"modise product id {prod_number} already exist! (passing)")
            continue
        target = f"https://www.modiseh.com/appservices/catalog_product/getdetail"
        job = {
            "url": target,
            "body": {"customer_id":0,"quote_id":None,"product_id":str(prod_number)}
        }
        jobs_buffer.append(job)
        if len(jobs_buffer) > 3:
            result_batch = asyncio.run(fetch(jobs_buffer))
            mongo_insert(mongo_client,result_batch)
            jobs_buffer = []
            time.sleep(int(SLEEPING))

