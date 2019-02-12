#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/29 11:03
from pyspark.sql import SparkSession,DataFrame
import pandas as pd
import threading
import json
import os
import re
import traceback as tb
from lib.utils import hdfsClient
import time
from lib.utils.config import *
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


def write(name_operation, exp_path):
    threads = {}
    for key in name_operation.keys():
        data = name_operation[key]

        try:
            th = multi_Thread(data=data, exp_path=exp_path, opt_name=key)
            th.start()
            threads[key] = th
        except Exception:
            tb.print_exc()
    for thread in threads.keys():
        threads[thread].join()


class multi_Thread(threading.Thread):
    def __init__(self, data, exp_path, opt_name):
        threading.Thread.__init__(self)
        self.data = data
        self.exp_path = exp_path
        self.opt_name = opt_name

    def run(self):
        print(f"start write {self.opt_name}-----------{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")
        if type(self.data) == DataFrame:
            start1 = time.time()
            # 删除类型为向量的列
            type_list = self.data.dtypes
            drop = []
            for tp in type_list:
                if tp[1] == 'vector':
                    drop.append(tp[0])
            self.data = self.data.drop(*drop)

            # 取前一百条
            self.data = self.data.limit(100)
            self.data = self.data.repartition(1)

            # 获取hdfs client
            cli = hdfsClient.get_hdfs_Client()
            # 目录是否存在
            if not cli.status(hdfs_path=self.exp_path, strict=False):
                cli.makedirs(self.exp_path)

            end1 = time.time()
            print(f"处理:{end1-start1}s")

            start2 = time.time()
            path = hdfs_host + self.exp_path + str(self.opt_name)
            self.data.write.csv(path=path,
                          mode="overwrite", header=True)

            end2 = time.time()
            print(f"write time:{end2-start2}")
        #评估的结果
        elif type(self.data) == dict:
            start = time.time()
            self.data = [self.data]
            # 使用hdfs模块
            cli = hdfsClient.get_hdfs_Client()
            path = self.exp_path + str(self.opt_name) + f"/{str(self.opt_name)}.json"
            cli.write(hdfs_path=path,
                      data=json.dumps(self.data), overwrite=True)
            end = time.time()
            print(f"write time:{end-start}")
        #模型
        else:
            # start = time.time()
            # path = hdfs_host + self.exp_path + str(self.opt_name)
            # self.data.write().overwrite().save(path)
            # end = time.time()
            # print(f"write time:{end-start}")
            pass
        print(f"end write {self.opt_name}-----------{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")


def read_tempData(file_path):
    """

    :param file_path:
    :return:  pandas's DataFrame
    """


    #调试
    # file_path = "temp/test/model08_train.csv.json"

    cli = hdfsClient.get_hdfs_Client()

    # 数据源文件
    if file_path.endswith(".csv"):
        with cli.read(hdfs_path=f"{file_path}") as file:
            df = pd.read_csv(file)
    else:
        for i in cli.list(file_path):
            if re.findall(r".*(.csv)$", i):
                with cli.read(hdfs_path=f"{file_path}/{i}") as file:
                    df = pd.read_csv(file)
            # 评估的数据
            elif re.findall(r".*(.json)$", i):
                with cli.read(hdfs_path=f"{file_path}/{i}") as file:
                    df = pd.read_json(file)
    return df
