# -*- coding: utf-8 -*-

from lib.Operations.OperationBase import OperationBase
from pyspark.sql import SparkSession


class ReaderBase(OperationBase):
    """
    Abstract class
    achieve by subclass in Reader
    传入参数必须包含：
            读入文件的路径
    """

    def __init__(self, data: dict, elem_dict: dict, spark):
        super().__init__(data, elem_dict, spark)
        self.file_path = self.args["filePath"]
        self.file_format = self.get_arg("fileFormat","csv")
