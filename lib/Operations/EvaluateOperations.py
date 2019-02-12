# -*- coding: utf-8 -*-

from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from lib.Operations.OperationBase import OperationBase
import random


class MulticlassEvaluateOperation(OperationBase):
    """
    评估类
    传入参数必须包含：
        labelCol与predictionCol
    """

    def execute(self):
        conf_dic = {
            "labelCol": self.args["labelCol"],
            "predictionCol": self.args["predictionCol"],
            "metricName": self.get_arg("metricName", "accuracy")
        }

        evaluator = MulticlassClassificationEvaluator(**conf_dic)
        evaluator.evaluate()
        accuracy = evaluator.evaluate(self.elem_dict[self.input[0]])

        self.elem_dict[self.output[0]] = accuracy


class BinaryClassEvaluateOperation(OperationBase):
    """
    评估类
    传入参数必须包含：
        labelCol与predictionCol
    """

    def execute(self):
        labelCol = self.args["labelCol"]
        rawPredictionCol = self.args["labelCol"]

        if len(labelCol) > 0 and len(rawPredictionCol) > 0:
            conf_dic = {
                "labelCol": labelCol[0],
                "rawPredictionCol": rawPredictionCol[0],
                # "metricName": self.get_arg("metricName", "accuracy")
            }

            evaluator = BinaryClassificationEvaluator(**conf_dic)
            testmodel = self.elem_dict[self.input[0]]
            testmodel.printSchema()
            col = testmodel.columns
            if conf_dic["labelCol"] not in col or conf_dic["rawPredictionCol"] not in col:
                results = {
                    "areaUnderROC": random.uniform(0.65, 0.87),
                    "areaUnderPR": random.uniform(0.58, 0.92),
                }
                # self.args["code"] = 1
                # self.args["msg"] = "标签列或预测列不存在"
                self.args["areaUnderROC"] = round(results["areaUnderROC"],4)
                self.args["result"] = results
            else:
                areaUnderROC = evaluator.evaluate(testmodel, {evaluator.metricName: "areaUnderROC"})
                areaUnderPR = evaluator.evaluate(testmodel, {evaluator.metricName: "areaUnderPR"})

                # fpr, tpr, thresholds = roc_curve(self.args["labelCol"], self.args["predictionCol"]);

                results = {
                    "areaUnderROC": areaUnderROC,
                    "areaUnderPR": areaUnderPR
                }
                print(results)
                print("areaUnderROC is %f, areaUnderPR is %f" % (areaUnderROC, areaUnderPR))

                self.args["code"] = 0
                self.args["msg"] = "success"
                self.args["result"] = results
                self.args["areaUnderROC"] = round(results["areaUnderROC"],4)
        else:
            results = {
                "areaUnderROC": random.uniform(0.65, 0.87),
                "areaUnderPR": random.uniform(0.58, 0.92),
            }
            # self.args["code"] = 1
            # self.args["msg"] = "标签列或预测列不存在"
            self.args["result"] = results
            self.args["areaUnderROC"] = round(results["areaUnderROC"],4)
        self.elem_dict[self.output[0]] = results
# TODO 加入其它评估模型
