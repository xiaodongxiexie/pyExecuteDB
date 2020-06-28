# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/6/28

from pyspark.sql import SparkSession
from pyspark.sql import types


class Spark:

    def __init__(self, name="demo"):
        self.name = name

    def __enter__(self):
        self.spark = (
            SparkSession.builder
                        .appName(self.name)
                        .getOrCreate()
        )
        return self.spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()


if __name__ == '__main__':

    with Spark("kudo-demo") as spark:
        kudu_master = ",".join(['192.168.1.1:7051', '192.168.1.2:7051'])
        kudu_table = "impala::database.from_table"
        kudu_table2 = "impala::database.to_table"
        frame = (
            spark.read
                 .format("org.apache.kudu.spark.kudu")
                 .option("kudu.master", kudu_master)
                 .option("kudu.table", kudu_table)
                 .load()
        )



        (
            frame.write
                 .format('org.apache.kudu.spark.kudu')
                 .option('kudu.master', kudu_master)
                 .mode('append')
                 .option('kudu.table', kudu_table2)
                 .option('kudu.option', 'upsert')
                 .save()
        )

