# -*- coding: utf-8 -*-

from lib.Operations.OperationBase import OperationBase
from pyspark.sql import DataFrame,Row,functions,Window
from pyspark.ml.feature import StringIndexer, VectorIndexer,Word2Vec,VectorAssembler


class SplitOperation(OperationBase):
    """
    split DataFrame into 2 part
    weight在args中传入，默认为[0.5: 0.5]
    """
    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        weight = self.get_arg("weight", [0.5, 0.5])
        (self.elem_dict[self.get_arg("outLeft")], self.elem_dict[self.get_arg("outRight")]) \
            = df_in.randomSplit(weight)


class StringIndexOperation(OperationBase):
    """
    将字符列转为int型
    args中必须存在input_col与output_col参数，代表需要转换的列与输出列名
    """

    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        # 需要转换的列
        input_col = self.get_arg("inputCol")
        output_col = self.get_arg("outputCol")
        assert (input_col is not None and output_col is not None)

        conf_dict = {
            "inputCol": input_col,
            "outputCol": output_col,
            "handleInvalid": self.get_arg("handleInvalid", "error")
        }

        string_indexer = StringIndexer(**conf_dict)
        self.elem_dict[self.output[0]] = string_indexer.fit(df_in).transform(df_in)


class VectorIndexOperation(OperationBase):
    """
    indexing categorical feature columns in a DataSet of `Vector`
    args中必须存在input_col与output_col参数
    """

    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        # 需要转换的列
        input_col = self.get_arg("inputCol")
        output_col = self.get_arg("outputCol")
        assert (input_col is not None and output_col is not None)

        conf_dict = {
            "inputCol": input_col,
            "outputCol": output_col,
            "handleInvalid": self.get_arg("handleInvalid", "error"),
            "maxCategories": self.get_arg("maxCategories", 20)
        }
        vector_indexer = VectorIndexer(**conf_dict)
        self.elem_dict[self.output[0]] = vector_indexer.fit(df_in).transform(df_in)

class AddIndex(OperationBase):
    """
         给数据集增加一列索引列
    """

    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        # 索引列的名称
        index_name = self.get_arg("IDColName", "index")

        w = Window.orderBy(df_in.columns[0])
        df_result = df_in.withColumn(index_name, functions.row_number().over(w))
        self.elem_dict[self.output[0]]=df_result

class VectorAssemblerOperation(OperationBase):
    """
    """
    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        # 需要向量化的列
        input_cols = self.get_arg("inputCol")
        output_col = self.get_arg("outputCol")
        assert (input_cols is not None and output_col is not None)
        assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col)
        result = assembler.transform(df_in)
        self.elem_dict[self.output[0]] = result
        result.show(10)
        result.printSchema()


class Word2VecOperation(OperationBase):
    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        # 需要转换的列
        input_col = self.get_arg("inputCol")
        output_col = self.get_arg("outputCol")
        assert (input_col is not None and output_col is not None)
        conf_dict={
            "inputCol": input_col,
            "outputCol": output_col,
            "vectorSize": self.get_arg("vectorSize",100),
            "windowSize": self.get_arg("windowSize",5),
            "numPartitions": self.get_arg("numPartitions",1),
            "maxIter": self.get_arg(" maxIter",1),
            "stepSize": self.get_arg("stepSize",0.025)
        }
        word2Vec=Word2Vec(**conf_dict)
        model=word2Vec.fit(df_in)
        result=model.transform(df_in)
        print(type(result))
        result.show(10)


class SamplingOperation(OperationBase):
    """
    sampling of input DataSet
    weight在args中传入，默认为[0.1]
    samplesize :    采样个数
    sampleRatio:	采样比例, 默认为0.1
    replace:	可选，是否放回，boolean类型
    randomSeed	随机数种子

    """

    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        print("enter into sampling")
        sampleSize = self.get_arg("sampleSize", 0)
        sampleRatio = self.get_arg("sampleRatio", 0.1)
        replace = self.get_arg("replace", False)
        randomSeed = self.get_arg("randomSeed", 1234)
        if sampleSize > 0:
            ratio = sampleSize / df_in.count()
        df_out = df_in.sample(withReplacement=replace, fraction=sampleRatio, seed=randomSeed)
        print("sampleRation == ")
        print(df_out.head())

        # self.elem_dict[self.get_arg("outLeft")] = df_out
        self.elem_dict[self.output[0]] = df_out


class FillnaOperation(OperationBase):
    """
    fill na of input DataSet
    selectedColNames	输入表选择列
    preValue	        原值（被替换值）
    fillValue	    	填充值，取值范围（max，min，avg， mean，0，1）

    """

    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        print("enter into fillna")
        col = self.get_arg("selectedColNames")
        preValue = self.get_arg("preValue")
        fillValue = self.get_arg("fillValue")
        cols = df_in.columns
        print(cols)
        if cols.__contains__(col):
            # df_out = df_in.na.fill({col: fillValue})
            pass
        else:
            self.elem_dict[self.output[0]] = "errorcodexxx"
            return 'errorcodexxx'

        label = fillValue + '(' + col + ')'

        # if fillValue in ('max', 'min', 'avg'):
        #     self.set_arg(self, "selectedColtoDouble", col)
        #     TypeTransformOperation.execute(self)
        #     df_in: DataFrame = self.elem_dict[self.get_arg("outLeft")]

        if fillValue == 'max':
            fillValue = df_in.groupby().max(col).collect()[0].asDict()[label]
        if fillValue == 'min':
            fillValue = df_in.groupby().min(col).collect()[0].asDict()[label]
        if fillValue == 'avg':
            fillValue = df_in.groupby().avg(col).collect()[0].asDict()[label]
        # check if type matches
        #type(fillValue) != type(df_in(col))
        print(str(fillValue) + " is " + label)

        df_out = df_in.fillna(fillValue, col)

        print(df_out.printSchema())
        print(df_out.head())

        self.elem_dict[self.get_arg("outLeft")] = df_out
        self.elem_dict[self.output[0]] = "fillna"


class TypeTransformOperation(OperationBase):
    """
    single column type transform of input DataSet
    selectedColtoInt	输入表选择列,转换为int列
    selectedColtoDouble	输入表选择列,转换为double列
    selectedColtoString	输入表选择列,转换为String列
    defaultIntVal	    转换为int异常时的默认填充值, default 0
    defaultDoubleVal	转换为double异常时的默认填充值, default 0
    defaultStringVal	转换为string异常时的默认填充值, default ""

    """

    def execute(self):
        df_in: DataFrame = self.elem_dict[self.input[0]]
        print("enter into TypeTransform")
        selectedColtoInt = self.get_arg("selectedColtoInt")
        selectedColtoDouble = self.get_arg("selectedColtoDouble")
        selectedColtoString = self.get_arg("selectedColtoString")
        defaultIntVal = self.get_arg("defaultIntVal", 0)
        defaultDoubleVal = self.get_arg("defaultDoubleVal", 0.0)
        defaultStringVal = self.get_arg("defaultStringVal", "")
        cols = df_in.columns
        print(cols)
        print(selectedColtoDouble)
        print(type(selectedColtoDouble))
        if selectedColtoInt is not None:
            coltoInt = []
            for i in selectedColtoInt:
                coltoInt.append(i.get('field'))
            print(coltoInt)
            if set(cols) > set(coltoInt):
                for i in coltoInt:
                    df_in = df_in.withColumn(i, df_in[i].cast("int"))
            else:
                print( "coltoInt not in data file")

        if selectedColtoDouble is not None:
            coltoDouble = []
            for i in selectedColtoDouble:
                coltoDouble.append(i['field'])
            print(coltoDouble)

            if set(cols) > set(coltoDouble):
                for i in coltoDouble:
                    df_in = df_in.withColumn(i, df_in[i].cast("double"))
            else:
                print( "selectedColtoDouble + not in data file")

        if selectedColtoString is not None:
            coltoString = []
            for i in selectedColtoString:
                coltoString.append(i['field'])
            print(coltoString)

            if set(cols) > set(coltoString):
                for i in coltoString:
                    df_in = df_in.withColumn(i, df_in[i].cast("string"))
            else:
                print("selectedColtoString + not in data file")

        df_out = df_in
        print(df_out.printSchema())
        print(df_out.head())

        self.elem_dict[self.get_arg("outLeft")] = df_out
        self.elem_dict[self.output[0]] = df_out
