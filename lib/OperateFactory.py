# -*- coding: utf-8 -*-
from lib.NameToHandler import name_to_handler
from lib.Operations import OperationBase
from pyspark.sql import SparkSession


class OperateFactory:
    """
    工厂类
    根据操作名与参数返回真正执行操作的对象
    """

    @classmethod
    def initial(cls, operation_info: dict, relations: dict, spark: SparkSession)-> OperationBase:
        """
        :param operation_info:  本次操作的所有信息，对应CoreApp中workflow列标的一项
        :param relations:   元素名与实际对象的映射，对应CoreApp中的name_operation对象
        :param spark: SparkSession实例
        :return: OperationBass的子类，实际进行操作的对象
        """
        operation_name = operation_info["action"]
        name_class = name_to_handler[operation_name]
        return name_class(operation_info, relations, spark)
