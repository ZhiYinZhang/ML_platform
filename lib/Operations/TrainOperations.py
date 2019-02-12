# -*- coding: utf-8 -*-
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.sql import DataFrame
from lib.Operations.OperationBase import OperationBase
from lib.Models.TrainModels import *


class TrainOperation(OperationBase):
    """
    训练模型操作
    arg中必须需要有以下参数
        model_name: 模型名称
    """

    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        col = df_in.columns
        #labelColName: list
        if len(self.get_arg("labelColName")) == 0 or self.get_arg("labelColName")[0] not in col:
            if 'label' in col:
                self.set_arg('labelColName', 'label')
                col.remove('label')
            else:
                lab = col[-1]
                self.set_arg('labelColName', lab)
                col.remove(lab)
            self.set_arg('featureColNames', col)
        else:
            # 把labelColName list->string
            self.set_arg("labelColName", self.get_arg("labelColName")[0])
            if self.get_arg('featureColNames') not in col:
                col.remove(self.get_arg('labelColName'))
                self.set_arg('featureColNames', col)

        if "features" not in col:
            # index the string label
            for column_type in df_in.dtypes:
                if column_type[0] in self.get_arg("featureColNames"):
                    if column_type[1] == 'string':
                        df_in = StringIndexer(inputCol=column_type[0], outputCol=column_type[0] + "_index",handleInvalid='skip')\
                            .fit(df_in)\
                            .transform(df_in)
                        self.args['featureColNames'].remove(column_type[0])
                        self.args['featureColNames'].append(column_type[0] + "_index")
            # all features need to be vectors in a single column, usually named features
            vecAssembler = VectorAssembler(inputCols=self.get_arg("featureColNames"), outputCol="features")
            self.set_arg('featureColNames', 'features')
            df_in = vecAssembler.transform(df_in)
        else:
            self.set_arg('featureColNames','features')

        # 根据参数初始化模型
        model = get_model_object(self.args)
        #调试
        print(type(model))
        # print(model.fit(df_in).intercept)
        self.elem_dict[self.output[0]] = model.fit(df_in)
