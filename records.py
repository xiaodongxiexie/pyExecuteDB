# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/8/12

# pip install records

from records import Connection, Database, Record, RecordCollection
from sqlalchemy import text


def iquery(self, query, batches=100):
    cursor = self._conn.execute(text(query))

    columns = cursor.keys()
    history = []
    for i, row in enumerate(cursor, start=1):
        history.extend(
            list(
                RecordCollection(
                    (Record(columns, _row) for _row in (row,))
                )
            )
        )
        if i % batches == 0:
            yield history
            history.clear()
    if history:
        yield history

def i2query(self, query, batches=100):
    with self.get_connection() as conn:
        for rows in conn.iquery(query, batches=batches):
            yield rows


def __str__(self):
    return "<Record {}>".format(self.as_dict())


Connection.iquery = iquery
Database.iquery = i2query
Record.__str__ = __str__
Record.__repr__ = __str__


if __name__ == '__main__':

    import os
    import psutil

    url = "your-url-engine"

    current_memoery_use = lambda: psutil.Process(os.getpid()).memory_info().rss/1024/1024
    
    print(current_memoery_use())
    database = Database(db_url=url)
    r = database.iquery("select * from your-schema.your-table", batches=100)
    print(current_memoery_use())


    for obj in r:
        # 批式计算
        # do_something_partial_by_batch(...)
        pass
        
    print(current_memoery_use())
