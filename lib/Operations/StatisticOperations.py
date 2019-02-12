#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import pandas as pd
from lib.Operations.OperationBase import OperationBase
from lib.utils.fileOperate import *


class Return100(OperationBase):
    """
       返回前一百条数据
    """
    def execute(self):
      if len(self.output) ==1: 
        file_path=self.output[0]
        print(type(file_path),file_path)

        file_path="multiCol.csv"
        df_in=readFile(self.spark,file_path)
        #[Row(),Row()....]
        data_row=df_in.take(100)
        data_dict=[]
        for i in data_row:
            data_dict.append(i.asDict())
        self.elem_dict[self.output[0]]=data_dict

        # 返回码  成功：0 失败：1
        self.args["code"] = 0
        self.args["msg"] = "success"
        self.args["result"] = data_dict
      else:
               
                self.args["code"] = 2
                self.args["msg"] = "输入数据集为空"
                self.args["result"] = ""


class Dataset_columns(OperationBase):
    """
        返回字段类型  格式：[{'column':column1Name,'type':column1Type},{'column':column2Name,'type':column2Type}]
    """
    def execute(self):
       if len(self.output) ==1:
         file_name=self.output[0]

         file_name="multiCol.csv"
         df_in=readFile(self.spark,file_name)
         types=df_in.dtypes
         result=[]

         for i in types:
             result.append({"column":i[0],"type":i[1]})

         self.elem_dict[self.output[0]]=result

         self.args["code"] = 0
         self.args["msg"] = "success"
         self.args["result"] = result
       else:
            
                self.args["code"] = 2
                self.args["msg"] = "输入数据集为空"
                self.args["result"] = ""


class Dataset_input_columns(OperationBase):
        """
            返回字段类型      [{'cols':['column1','column2'],'fieldType':'STRING'},{'cols':['column3','column4'],'fieldType':'BIGINT'}]
        """

        def execute(self):
            print(len(self.output))
            if len(self.output) ==1:
                file_name = self.output[0]

                file_name = "multiCol.csv"
                df_in = readFile(self.spark, file_name)
                #[('col1', 'bigint'), ('col2', 'string'), ('col3', 'bigint')]
                types = df_in.dtypes

                result = []
                d = {}
                for i in types:
                    type = i[1]
                    if type in d.keys():
                        d[type].append(i[0])
                    else:
                        d[type] = [i[0]]

                for key in d.keys():
                    r = {'cols': d[key], 'fieldType': key}
                    result.append(r)

                self.elem_dict[self.output[0]] = result
                self.args["code"] = 0
                self.args["msg"] = "success"
                self.args["result"] = result
            else:
                  
                self.args["code"] = 2
                self.args["msg"] = "输入数据集为空"
                self.args["result"] = ""
                print(self.args)


class Return100_py():
    """
       返回前一百条数据
    """
    def __init__(self, data_args: dict):
            self.data_args = data_args

    def execute(self):

        file = self.data_args['file_path']
        df = read_tempData(file_path=file)
        self.data_args['process'][0]["code"] = 0
        self.data_args['process'][0]["msg"] = "success"
        self.data_args['process'][0]["result"] = df[0:100].to_dict(orient='record')


#pandas的DataFrame的dtype类型 转成前端显示的类型
type_map = {
    'object'  : 'string',
    'int64'   : 'int',
    'float64' : 'double',
    'bool'    : 'boolean',
}

class Dataset_columns_py():
    """
           返回字段数据类型       格式：[{'column':column1Name,'type':column1Type},{'column':column2Name,'type':column2Type}]
        """

    def __init__(self, data_args: dict):
        self.data_args = data_args

    def execute(self):
        file = self.data_args['file_path']
        results = []

        df = read_tempData(file_path=file)
        types = df.dtypes

        ty_dict = types.to_dict()
        for key in ty_dict.keys():
            pd_type = ty_dict[key].name
            ty_dict[key] = type_map.get(pd_type,pd_type)

            result = {"column":key,"type":ty_dict[key]}
            results.append(result)

        self.data_args['process'][0]["code"] = 0
        self.data_args['process'][0]["msg"] = "success"
        self.data_args['process'][0]["result"] = results            

class Dataset_input_columns_py():
        """
            返回字段类型      [{'cols':['column1','column2'],'fieldType':'STRING'},{'cols':['column3','column4'],'fieldType':'BIGINT'}]
        """

        def __init__(self, data_args: dict):
            self.data_args = data_args

        def execute(self):
            file = self.data_args['file_path']


            df = read_tempData(file_path=file)
            #{'col1': 'int', 'col2': 'int', 'col3': 'double'}
            ty_dict = df.dtypes.to_dict()
            #将pandas的DataFrame里的type转化成页面展示的类型
            for key in ty_dict.keys():
                #获取数据类型
                pd_type = ty_dict[key].name
                #映射
                ty_dict[key] = type_map.get(pd_type, pd_type)
            #{'int': ['col1', 'col2'], 'double': ['col4', 'col3'], 'string': ['col5', 'col6']}
            temp = {}
            for colName in ty_dict.keys():
                colType = ty_dict[colName]
                if colType in temp.keys():
                    temp[colType].append(colName)
                else:
                    temp[colType] = [colName]
            #[{'cols':['column1','column2'],'fieldType':'string'},{'cols':['column3','column4'],'fieldType':'int'}]
            results = []
            for key in temp.keys():
                r = {'cols': temp[key], 'fieldType': key}
                results.append(r)

            self.data_args['process'][0]["code"] = 0
            self.data_args['process'][0]["msg"] = "success"
            self.data_args['process'][0]["result"] = results

class Evaluate_data():

    def __init__(self, data_args: dict):
        self.data_args = data_args

    def execute(self):
        file = self.data_args['file_path']
        df = read_tempData(file_path=file)

        list_json = df.to_dict(orient='record')
        results = []
        for j in list_json:
            for key in j.keys():
                results.append({"key": key, "value": j[key]})

        self.data_args['process'][0]["code"] = 0
        self.data_args['process'][0]["msg"] = "success"
        self.data_args['process'][0]["result"] = {"evaluateResult": results}


if __name__ == '__main__':
    data = {
        "data": {
            "expId": "EA96AF677276F9DA54865C8EA8F8AA61"
        },
        "headers": {
            "code": 0,
            "expId": "EA96AF677276F9DA54865C8EA8F8AA61",
            "identifier": "exp_execute_all",
            "msg": "",
            "sessionId": "285188972D40C8B2119C26D682DDFCF",
            "userId": "123321"
        },
        "process": [
            {
                "inputDatasets": [],
                "outputDatasets": ["test.json"],
                "action": "dataset_input_columns_py"
            }
        ],
        "file_path": ["test.json"]
    }
    Dataset_input_columns_py(data).execute()
    print(data)
