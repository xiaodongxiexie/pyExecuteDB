# redis

import redis
import tqdm

def redis_con(host, port, passwd, use_pool=False):

    if not use_pool:
        rds = redis.Redis(host=host, port=port, db=0, password=passwd)
    else:
        pool = redis.ConnectionPool(host=host, port=port, db=0, password=passwd, decode_responses=True)
        rds = redis.Redis(connection_pool=pool)
        
    return rds


class Rds:

    def expire(self, rds: redis.Redis, k: str, expire_seconds: int) -> None:
        """
        :param rds:   redis 实例
        :param k:      键
        :param expire_seconds: 过期时间（秒）
        :return: No Return
        """
        rds.expire(k, expire_seconds)
    
    def pipeline(self, rds: redis.Redis, rets: dict, expire_seconds: int, batch: int = 1000) -> None:
        with rds.pipeline(transaction=False) as p:
            _c = 0
            for key, v in tqdm.tqdm(rets.items()):
                _c += 1
                p.zadd(key, v)
                p.expire(key, expire_seconds)
                if _c % batch == 0:
                    p.execute()
                    _c = 0
            if _c:
                p.execute()
             
