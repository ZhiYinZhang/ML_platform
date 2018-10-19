#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/29 11:03
from pyspark.sql import SparkSession,DataFrame
import pandas as pd
import json
import threading
import os
import traceback as tb
from lib.utils import hdfsClient
import time
# 读写中间数据

def readFile(spark: SparkSession, file_path):
    """
    :param spark:  sparkSession对象
    :param file_path:  文件路径
    :return:  DataFrame对象
    """

    df_in:DataFrame = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("path", file_path) \
        .load()
    return df_in


def write_hdfs(data,file_path):
    """

    :param data:
    :param file_path:
    """

    cli = hdfsClient.get_hdfs_Client()
    path = os.path.dirname(file_path)
    #目录是否存在
    if not cli.status(hdfs_path=path,strict=False):
        cli.makedirs(path)
    list_row = data
    #将Row转成dict
    list_json = []
    for i in list_row:
        d = i.asDict()
        list_json.append(d)
    start = time.time()
    cli.write(hdfs_path=file_path,data=json.dumps(list_json),buffersize=1024*1024,encoding='utf-8',overwrite=True,replication=1)
    end = time.time()
    print(f"write time:{end-start}")

class mulThread_write(threading.Thread):
    def __init__(self,data,file_path,name):
        threading.Thread.__init__(self)
        self.data = data
        self.file_path = file_path
        self.name = name

    def run(self):
        print("start write:----------------",self.name,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
        write_hdfs(data=self.data,file_path=self.file_path+f"{self.name}.json")
        print("end write:-----------------",self.name,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))


def write(name_operation, file_path):
    threads = {}
    for key in name_operation.keys():
        data = name_operation[key]
        if type(data) == DataFrame:
            try:
                th = myThread(df=data, file_path=file_path, file_name=key)
                th.start()
                threads['key'] = th
            except Exception:
                tb.print_exc()
    for thread in threads.keys():
        threads[thread].join()


class myThread(threading.Thread):
    def __init__(self, df: DataFrame, file_path, file_name):
        threading.Thread.__init__(self)
        self.df = df
        self.file_path = file_path
        self.file_name = file_name

    def run(self):
        print(f"start write {self.file_name}-----------{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")
        start1 = time.time()
        # 删除类型为向量的列
        type_list = self.df.dtypes
        drop = []
        for tp in type_list:
            if tp[1] == 'vector':
                # print(tp[0])
                drop.append(tp[0])
        self.df = self.df.drop(*drop)
        # 取前一百条
        list_row = self.df.take(100)
        end1 = time.time()
        print(f"处理:{end1-start1}s")

        # 写hdfs
        cli = hdfsClient.get_hdfs_Client()
        # 目录是否存在
        if not cli.status(hdfs_path=self.file_path, strict=False):
            cli.makedirs(self.file_path)
        # 将Row转成dict
        list_json = []
        for i in list_row:
            d = i.asDict()
            list_json.append(d)
        start = time.time()
        cli.write(hdfs_path=self.file_path + f"{self.file_name}.json", data=json.dumps(list_json),
                  buffersize=1024 * 1024, encoding='utf-8',
                  overwrite=True, replication=1)
        end = time.time()
        print(f"write time:{end-start}")

        print(f"end write {self.file_name}-----------{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")


def read_tempData(file_path):
    """

    :param file_path:
    :return:  pandas's DataFrame
    """


    #调试
    # file_path = "temp/test/model08_train.csv.json"

    cli = hdfsClient.get_hdfs_Client()

    with cli.read(hdfs_path=file_path) as file:
        if file_path.endswith(".json"):
               df = pd.read_json(file)
        elif file_path.endswith(".csv"):
               df = pd.read_csv(file)
    return df
