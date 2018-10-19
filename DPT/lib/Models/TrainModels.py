# -*- coding: utf-8 -*-

from pyspark.ml.classification import GBTClassifier
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import GBTClassifier,LogisticRegression,NaiveBayes,LinearSVC, RandomForestClassifier


class ModelBase(object):
    """
    模型基类，具体模型类通过实现get_model类方法来返回合适对象
    """
    @classmethod
    def get_model(cls, args: dict):
        pass


class GBTCModel(ModelBase):
    """
    GBTClassifier模型
    必选参数：
        labelCol
        featuresCol
    可选参数：
        maxIter 最大迭代次数，默认为10
    """
    @classmethod
    def get_model(cls, args: dict):
        conf_dict = {
            "featuresCol": args["featureColNames"],
            "labelCol": args["labelColName"],
            "predictionCol": args.get("predictionCol", "prediction"),
            "maxDepth": args.get("maxDepth", 5),
            "maxBins": args.get("maxBins", 32),
            "minInstancesPerNode": args.get("minInstancesPerNode", 1),
            "minInfoGain": args.get("minInfoGain", 0.0),
            "maxMemoryInMB": args.get("maxMemoryInMB", 256),
            "cacheNodeIds": args.get("cacheNodeIds", False),
            "checkpointInterval": args.get("checkpointInterval", 10),
            "lossType": args.get("lossType", "logistic"),
            "maxIter": args.get("maxIter", 20),
            "stepSize": args.get("stepSize", 0.1),
            "seed": args.get("seed", None),
            "subsamplingRate": args.get("subsamplingRate", 1.0)
        }
        return GBTClassifier(**conf_dict)

class GBTRModel(ModelBase):
    """
    GBTRegressor模型
    必选参数：
        labelCol
        featuresCol
    可选参数：
        maxIter 最大迭代次数，默认为10
    """
    @classmethod
    def get_model(cls, args: dict):
        conf_dict = {
            "labelCol": args["labelColName"],
            "featuresCol": args["featureColNames"],
            "maxIter": args.get("maxIter", 10)
        }
        return GBTRegressor(**conf_dict)

class LogisticRegressionModel(ModelBase):
    """
       LogisticRegression模型
    """

    @classmethod
    def get_model(cls, args: dict):
        conf_dict = {
            "featuresCol": args.get("featureColNames", "features"),
            "labelCol": args.get("labelColName", "label"),
            # "predictionCol": args.get("predictionCol", "prediction"),
            "maxIter": args.get("maxIter", 100),
            "regParam": args.get("regParam", 0.0),
            # "elasticNetParam": args.get("elasticNetParam", 0.0),
            "tol": args.get("tol", 1e-06),
            # "fitIntercept": args.get("fitIntercept", True),
            # "threshold": args.get("threshold", 0.5),
            # "thresholds": args.get("thresholds", None),
            # "probabilityCol": args.get("probabilityCol", "probability"),
            # "rawPredictionCol": args.get("rawPredictionCol", "rawPrediction"),
            "standardization": args.get("standardization", True),
            # "weightCol": args.get("weightCol", None),
            "aggregationDepth": args.get("aggregationDepth", 2),
            "family": args.get("family", "auto"),
            # "lowerBoundsOnCoefficients": args.get("lowerBoundsOnCoefficients", None),
            # "upperBoundsOnCoefficients": args.get("upperBoundsOnCoefficients", None),
            # "lowerBoundsOnIntercepts": args.get("lowerBoundsOnIntercepts", None),
            # "upperBoundsOnIntercepts": args.get("upperBoundsOnIntercepts", None)
        }
        return LogisticRegression(**conf_dict)

class NaiveBayesModel(ModelBase):

    @classmethod
    def get_model(cls, args: dict):
        conf_dict = {
            "featuresCol": args.get("featureColNames", "features"),
            "labelCol": args.get("labelColName", "label"),
            "predictionCol": args.get("predictionCol", "prediction"),
            "probabilityCol": args.get("probabilityCol", "probability"),
            "rawPredictionCol": args.get("rawPredictionCol", "rawPrediction"),
            "smoothing": args.get("smoothing", 1.0),
            "modelType": args.get("modelType", "multinomial"),
            "thresholds": args.get("threshold", None),
            "weightCol": args.get("weightCol", None)
        }
        return NaiveBayes(**conf_dict)

class LinearSVMModel(ModelBase):
    """
      线性支持向量机模型
    """
    @classmethod
    def get_model(cls, args: dict):
        conf_dict = {
            "featuresCol": args.get("featureColNames", "features"),
            "labelCol": args.get("labelColName", "label"),
            "predictionCol": args.get("predictionCol", "prediction"),
            "maxIter": args.get("maxIter", 100),
            "regParam": args.get("regParam", 0.0),
            "tol": args.get("tol", 1e-06),
            "rawPredictionCol": args.get("rawPredictionCol", "rawPrediction"),
            "fitIntercept": args.get("fitIntercept", True),
            "standardization": args.get("standardization", True),
            "threshold": args.get("threshold", 0.5),
            # "weightCol": args.get("weightCol", None),
            "aggregationDepth": args.get("aggregationDepth", 2)
        }
        return LinearSVC(**conf_dict)

class RandomForestModel(ModelBase):
    """
    RandomForestModel模型
    必选参数：
        labelCol
        featuresCol
    可选参数：
        maxIter 最大迭代次数，默认为10
    """
    @classmethod
    def get_model(cls, args: dict):
        conf_dict = {
            "labelCol": args["labelColName"],
            "featuresCol": args["featureColNames"],
            "numTrees": args.get("numTrees", 10)
        }
        return RandomForestClassifier(**conf_dict)


# 模型名与模型类的映射
name_to_model = {
    "GBDT_model": GBTCModel,
    "GBTR": GBTRModel,
    "LR_model": LogisticRegressionModel,
    "SVM_model": LinearSVMModel,
    "Bayes_model": NaiveBayesModel,
    "RF_model": RandomForestModel
}


# 根据操作参数初始化用户想要的模型
def get_model_object(args):
    return name_to_model[args["model_name"]].get_model(args)
