# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/4/4


import csv


def read_csv(path, max_row=100):
    with open(path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, 1):
            if i > max_row:
                break
            yield row
