#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/29 11:03
from pyspark.sql import SparkSession,DataFrame
import pandas as pd
import json
import re
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

def write(name_operation, file_path):
    threads = {}

    for key in name_operation.keys():
        data = name_operation[key]
        if type(data) == DataFrame:
            try:
                th = myThread(df=data, file_path=file_path, file_name=key)
                th.start()
                threads[key] = th
            except Exception:
                tb.print_exc()
    #等待所有线程结束
    for thread in threads.keys():
        threads[thread].join()

#spark写入hdfs
class myThread(threading.Thread):
        def __init__(self, df: DataFrame, file_path, file_name):
            threading.Thread.__init__(self)
            threading.Thread.setName(self, name=file_name)
            self.df: DataFrame = df
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
            self.df = self.df.limit(100)
            self.df = self.df.repartition(1)

            # 获取hdfs client
            cli = hdfsClient.get_hdfs_Client()
            # 目录是否存在
            if not cli.status(hdfs_path=self.file_path, strict=False):
                cli.makedirs(self.file_path)

            end1 = time.time()
            print(f"处理:{end1-start1}s")

            start2 = time.time()
            path = "hdfs://10.18.0.11:8020/user/zhangzy/DPT/" + self.file_path + str(self.file_name)
            self.df.write.csv(path=path,
                              mode="overwrite", header=True)
            end2 = time.time()
            print(f"write time:{end2-start2}")
            print(f"end write {self.file_name}-----------{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")

#hdfs模块写入hdfs
# class myThread(threading.Thread):
#     def __init__(self, df: DataFrame, file_path, file_name):
#         threading.Thread.__init__(self)
#         self.df = df
#         self.file_path = file_path
#         self.file_name = file_name
#
#     def run(self):
#         print(f"start write {self.file_name}-----------{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")
#         start1 = time.time()
#         # 删除类型为向量的列
#         type_list = self.df.dtypes
#         drop = []
#         for tp in type_list:
#             if tp[1] == 'vector':
#                 # print(tp[0])
#                 drop.append(tp[0])
#         self.df = self.df.drop(*drop)
#         # 取前一百条
#         list_row = self.df.take(100)
#         end1 = time.time()
#         print(f"处理:{end1-start1}s")
#
#         # 写hdfs
#         cli = hdfsClient.get_hdfs_Client()
#         # 目录是否存在
#         if not cli.status(hdfs_path=self.file_path, strict=False):
#             cli.makedirs(self.file_path)
#         # 将Row转成dict
#         list_json = []
#         for i in list_row:
#             d = i.asDict()
#             list_json.append(d)
#         start = time.time()
#         cli.write(hdfs_path=self.file_path + f"{self.file_name}.json", data=json.dumps(list_json),
#                   buffersize=1024 * 1024, encoding='utf-8',
#                   overwrite=True, replication=1)
#         end = time.time()
#         print(f"write time:{end-start}")
#
#         print(f"end write {self.file_name}-----------{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")


def read_tempData(file_path):
    """

    :param file_path:
    :return:  pandas's DataFrame
    """

    cli = hdfsClient.get_hdfs_Client()

    #数据源文件
    if file_path.endswith(".csv"):
        with cli.read(hdfs_path=f"{file_path}") as file:
           df = pd.read_csv(file)
    else:
        for i in cli.list(file_path):
            if re.findall(r".*(.csv)$", i):
                with cli.read(hdfs_path=f"{file_path}/{i}") as file:
                    df = pd.read_csv(file)
    return df

