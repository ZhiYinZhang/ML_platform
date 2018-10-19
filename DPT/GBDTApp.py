# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def test():
    spark = SparkSession \
                .builder \
                .appName("GBDT test") \
                .master("local[2]")\
                .getOrCreate()

    # data = spark.read.format("libsvm").load("E:\\data\\sample_libsvm_data.txt").cache()
    data = spark.read.format("libsvm").load("files/sample_libsvm_data.txt").cache()
    data.show(10)

    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Train a GBT model.
    gbt = GBTClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", maxIter=10)
    # Chain indexers and GBT in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, gbt])

    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)
    predictions.select("prediction", "indexedLabel", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g" % ((1.0 - accuracy)*100) + "%")

    gbtModel = model.stages[2]
    print(gbtModel)


if __name__ == '__main__':
    test()
