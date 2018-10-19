# -*- coding: utf-8 -*-
from pyspark.ml.feature import VectorAssembler,StringIndexer
from lib.Operations.ReaderBase import ReaderBase
#调试
from pyspark.ml.linalg import Vectors
import os
from pyspark.sql import Row

class HdfsReader(ReaderBase):
    """
    HdfsReader
    用于读取Hdfs文件与本地文件
    """

    def execute(self):
        reader = self.spark.read.format(self.file_format)
        # 是否自动推导类型

        reader = reader.option("inferSchema",self.get_arg("inferSchema","true"))
        # 是否手动设置了文件分隔符
        if self.get_arg("delimiter") is not None:
            reader = reader.option("delimiter", self.get_arg("delimiter"))
        # if self.get_arg("header") is not None:
        reader=reader.option("header",self.get_arg("header","true"))


        # elem_dict对应CoreApp中的name_operation
        # 在此将输入元素对应为文件路径
        # 将输出元素对应为读取后的DataFrame
        # self.elem_dict[self.input[0]] = self.file_path
        df_in = reader.load(self.file_path)

        type_list = df_in.dtypes
        for i in type_list:
            if i[1] == "timestamp":
                df_in = df_in.withColumn(i[0], df_in[i[0]].cast("string"))

        df_in.printSchema()
        # reader.load()
        #数据集的名称作为key
        self.elem_dict[self.output[0]] = df_in



# TODO HiveReader, HbaseReader, MysqlReader
