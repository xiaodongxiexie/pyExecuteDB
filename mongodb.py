# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/4/3

import json

import requests
import pymongo
import redis

rdb = redis.Redis(db=10)
mongo_client = pymongo.MongoClient()

KEYS = [
    '0a92292ae8022c87c7c3d87d2c194519',
    '126b04d6de35dd00e83b96aa18714a8b',
    ]


def geocode(**kwargs):
    base = "https://restapi.amap.com/v3/geocode/geo?parameters"
    response = requests.get(base, kwargs)
    answer = response.json()
    return answer


def save_lnglat(addresses, rdb=None, mongo_table=None, keys=KEYS):
    _ret = {}
    keys = list(keys)
    key = keys.pop(0)
    for address in addresses:
        if rdb:
            if address in rdb:
                continue
        ret = geocode(key=key, address=address)
        while ret['info'] != "OK":
            if not keys:
                print("key 耗尽")
                break
            else:
                print("key 可用个数: {}".format(len(keys)))
            key = keys.pop(0)
            ret = geocode(key=key, address=address)
        try:
            longitude, latitude = ret['geocodes'][0]['location'].split(",")
        except:
            return
        lnglat = {}
        lnglat["longitude"] = longitude
        lnglat["latitude"] = latitude
        _ret[address] = lnglat
        if rdb:
            rdb.hset(address, "longitude", longitude)
            rdb.hset(address, "latitude", latitude)
        if mongo_table:
            mongo_table.insert_one(ret)
        print(_ret)
    return _ret
