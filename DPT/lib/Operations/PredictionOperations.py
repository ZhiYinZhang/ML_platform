# -*- coding: utf-8 -*-
from lib.Operations.OperationBase import OperationBase
import re
#调试
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

class PredictionOperation(OperationBase):
    """
    make prediction
    need args
        model: elem id of the trained model
        testSet: elem id of the testSet
    """
    def execute(self):
        for input in self.get_arg('inputDatasets'):
             if re.findall(r'.*(model).*',input):
                 model = input
             else:
                 test_set = input

        # model = self.get_arg("model")
        # test_set = self.get_arg("testSet")
        #调试
        # test_set=self.spark.createDataFrame([Row(features=Vectors.sparse(2, [0], [1.0]))])
        # transform
        self.elem_dict[self.output[0]] = self.elem_dict[model].transform(self.elem_dict[test_set])
