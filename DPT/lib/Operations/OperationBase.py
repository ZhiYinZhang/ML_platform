# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

class OperationBase(object):
    """
    OperationBase
    Abstract Class. Do not instantiate
    achieve execute in subclass

    input: input list
    output: output list
    args: operation args
    elem_dict: elem name to actual elem object

    get_arg: get a parameter
    execute: link the output to input. Not actually execute unless it is a action operation
    """

    def __init__(self, data: dict, elem_dict: dict, spark: SparkSession):
        self.input = data["inputDatasets"]
        self.output = data["outputDatasets"]
        self.args = data
        self.elem_dict = elem_dict
        self.spark = spark

    def get_arg(self, _str: str, default=None):
        return self.args.get(_str, default)

    def set_arg(self, _str: str, _value: str, default=None):
        self.args[_str] = _value
        return self.args


    def execute(self):
        pass
