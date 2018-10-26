# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from lib.OperateFactory import OperateFactory
import json
import time
import traceback as tb
from lib.utils.fileOperate import *
from lib.utils import write2mq


def test():
    file_name = "data.json"
    file_name = "E:/javacode/model/model02.json"
    with open(file_name) as f:
        data_args = json.loads(f.read())
    print(f"消息{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{data_args}")

    #将csv结尾换成parquet
    dataset = data_args['process'][0]['outputDatasets'][0]
    dataset = dataset.split(".")[0]+".parquet"


    filePath = os.path.join("e:/javacode", "dataset",dataset)
    # filePath = f"DPT/dataset/{dataset}"
    data_args["process"][0]["filePath"] = filePath
    # 工作流列表
    workflow = data_args["process"]
    # 元素名与实际对象的映射
    name_operation = {}
    # 初始化SparkSession
    spark = SparkSession \
        .builder \
        .appName("GBDT test") \
        .master("local[5]")\
        .getOrCreate()
    i = 0
    flag = True
    print('准备进入workflow')
    for step in workflow:
        print(i,step['action'])
        i += 1
        try:
            # 依工作流次序执行操作
            operation_instance = OperateFactory.initial(step, name_operation, spark)
            if flag:
                operation_instance.execute()
                if step.get("end_execute_node"):
                    flag = False
                step["code"] = 0
                step["msg"] = "success"
                if step.get("result", "") == "":
                    step["result"] = ""
            else:
                step["code"] = 2
                step["msg"] = "not run"
                step["result"] = ""
        except Exception as e:
            exc_info = tb.format_exc()
            print(exc_info)

            # 返回码  成功：0 失败：1
            step["code"] = 1
            step["msg"] = "参数错误"
            step["result"] = ""

            data_args["headers"]["code"] = 1
            data_args["headers"]["msg"] = "参数错误"
            flag = False

    # 本次试验所有的中间数据和结果的存储路径   userId/实验id/
    file_path = f"temp/{data_args['headers']['userId']}/{data_args['headers']['expId']}/"

    #必须在spark.stop()前执行
    start = time.time()
    write(name_operation=name_operation,file_path=file_path)
    end = time.time()
    print(f"总用时:{end-start}s")

    # 供调试用，结果显示在SparkHistory Driver的stdout中
    print("the result is:")
    # 定义写回RabbitMQ的字典
    result = {
        "data": data_args["data"],
        "headers": data_args["headers"],
        "process": data_args["process"]
    }
    # 写回结果到mq
    # write2mq.write(result)
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{result}")

    spark.stop()
    print("spark stop")

# 最终结果包含的类型
# 按需添加
final_res_set = {int, float, str, list}




# 判断元素是否为最后的结果
def is_final_res(instance):
    return type(instance) in final_res_set


if __name__ == '__main__':
    test()

