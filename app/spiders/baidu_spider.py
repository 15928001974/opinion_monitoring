# -*- conding:utf-8 -*-

import requests
import re
import time
import asyncio
from aiohttp import ClientSession


# async def get_page(i):
#     async with ClientSession() as session:
#             num_url = "http://tieba.baidu.com/p/2877306063?pn=" + str(i)
#             async with session.get(num_url, timeout=20) as resp:
#                 print('=====================')
#                 if re.search(r'金融', await resp.text()):
#                     print(num_url, '金融')
#
# loop = asyncio.get_event_loop()
# for i in range(1, 16):
#     task = asyncio.ensure_future(get_page(i))
# loop.run_until_complete(task)


# for i in range(16):
#     num_url = "http://tieba.baidu.com/p/2877306063?pn=" + str(i)
#     page = requests.get(num_url)
#     if re.search(r'金融', page.text):
#         print(num_url, '金融')


import multiprocessing


def subprocess(i):
    num_url = "http://tieba.baidu.com/p/2877306063?pn=" + str(i)
    page = requests.get(num_url)
    if re.search(r'金融', page.text):
        print(num_url, '金融')


def mainprocess():
    pool = multiprocessing.Pool()
    for i in range(9):
        pool.apply_async(subprocess, args=(i,))
    pool.close()
    pool.join()

if __name__ == '__main__':
    time1 = time.time()
    mainprocess()
    print(time.time() - time1)