# -*- coding: utf-8 -*-
from lib.Operations import *


# 操作名与实际操作类的映射
# 如果需要使用新建的操作类，务必规范好对应名称并加入到下面的字典中
name_to_handler: dict = {
    "train": TrainOperation,
    "dataset": HdfsReader,
    "stringIndex": StringIndexOperation,
    "vectorIndex": VectorIndexOperation,
    "split": SplitOperation,
    "predict": PredictionOperation,
    "evaluate": BinaryClassEvaluateOperation,
    "sampling": SamplingOperation,
    "fillna": FillnaOperation,
    "typetransform": TypeTransformOperation,
    "addIndex": AddIndex,
    "vectorAssembler": VectorAssemblerOperation,
    "word2vec": Word2VecOperation
}
#获取数据集信息 的操作名与实际操作类的映射
dataset_name_to_handler:dict = {
    "evaluate_data": Evaluate_data,
    "top_data": Return100_py,
    "dataset_columns": Dataset_columns_py,
    "dataset_input_columns": Dataset_input_columns_py
}