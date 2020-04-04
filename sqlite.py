# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/4/3

import os
import sqlite3

from python_executor_db.utils import (csv_reader,
                                      links_sql,
                                      tags_sql,
                                      movies_sql,
                                      ratings_sql)


# create
def create_table(table_path, create_table_sql):
    with sqlite3.connect(table_path) as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()


def execute_sql(table_path, sql):
    with sqlite3.connect(table_path) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)


# 增
# 单次插入
def insert_one(table_path, insert_table_sql):
    execute_sql(table_path, insert_table_sql)


# 批量插入
def insert_many(table_path, insert_table_sql, values):
    with sqlite3.connect(table_path) as conn:
        cursor = conn.cursor()
        cursor.executemany(insert_table_sql, values)


# 查
def select(table_path, select_table_sql, just_one=True):
    with sqlite3.connect(table_path) as conn:
        cursor = conn.cursor()
        cursor.execute(select_table_sql)
        # 查看具体各列的定义~
        # print(cursor.description)
        if just_one:
            return cursor.fetchone()
        else:
            return cursor.fetchall()


# 改
def update(table_path, update_table_sql):
    execute_sql(table_path, update_table_sql)


# 删
def delete(table_path, delete_table_sql):
    execute_sql(table_path, delete_table_sql)


if __name__ == "__main__":

    table_path = "data/dbs/movies.db"
    for create_table_sql in (links_sql, ratings_sql, tags_sql, movies_sql):
        create_table(table_path, create_table_sql)

    print(select(table_path, "select * from links"))

    insert_one(table_path, "insert into links values (1, 2, 3)")
    print(select(table_path, "select * from links"))

    update(table_path, "update links set movieId = 100 where movieId=1")
    print(select(table_path, "select * from links"))

    delete(table_path, "delete from links where movieId=100")
    print(select(table_path, "select * from links"))

    insert_many(table_path, "insert into links values (?, ?, ?)", [[1, 2, 3], [2, 3, 4], [4, 5, 6]])
    print(select(table_path, "select * from links"))

    delete(table_path, "delete from links")
    print(select(table_path, "select * from links"))

    dataset_path = "data/dataset"
    for table_name in ("links", "movies"):
        values = []
        for v in csv_reader.read_csv(os.path.join(dataset_path, f"{table_name}.csv")):
            values.append(list(v.values()))
        placeholder = "({})".format(",".join(["?"]*len(v)))
        insert_many(table_path, f"insert into {table_name} values {placeholder}", values)
    print(select(table_path, "select a.*, b.* from links a, movies b where a.movieId=b.movieId"))
