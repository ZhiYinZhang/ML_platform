#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/30 11:27
import time
import traceback as tb
from lib.NameToHandler import name_to_handler
from lib.utils import write2mq,hdfsClient


def Get_dataset_info(data_args):
    cli = hdfsClient.get_hdfs_Client()

    print(f"消息{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{data_args}")
    headers = data_args['headers']
    user_id = headers['userId']
    experiment_id = headers['expId']

    if user_id == "":
        user_id = f"Tourist_{experiment_id}"
    headers['userId'] = user_id

    #获取要读取的数据集名称
    output = data_args['process'][0]['outputDatasets'][0]
    if output.endswith('.csv') and output.startswith('model'):
        #数据源文件
        file = f"dataset/{output}"
    else:
        #中间文件
        file = f"temp/{user_id}/{experiment_id}/{output}"
    data_args['file_path'] = file

    if cli.status(hdfs_path=file,strict=False):
        # 操作
        action_info = data_args["process"][0]
        name_class = name_to_handler[action_info['action']]

        try:
           #执行
           name_class(data_args).execute()
        except Exception as e:
            exc_info = tb.format_exc()
            print(exc_info)
            # 返回码  成功：0 失败：1
            action_info["code"] = 1
            action_info["msg"] = exc_info.split("\n")[-2]
            action_info["result"] = ""
            #header中的错误码信息
            headers["code"] = 1
            headers["msg"] = exc_info.split("\n")[-2]

    else:
        data_args["headers"]["code"] = 1
        data_args["headers"]["msg"] = "数据集不存在"

        data_args['process'][0]["code"] = 1
        data_args['process'][0]["msg"] = "数据集不存在"
        data_args['process'][0]["result"] = ""
    # 供调试用，结果显示在SparkHistory Driver的stdout中
    print("the result is:")
    # print(json.dumps(data_args, indent=4))

    result={
        "data":data_args["data"],
        "headers":data_args["headers"],
        "process":data_args["process"]
    }

    #写回结果到mq
    # write2mq.write(result)
    print(f"结果{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{result}")


if __name__ == '__main__':
    # 调试
    dataTest = {
        "data": {},
        "headers": {
            "code": 0,
            "expId": "EA96AF677276F9DA54865C8EA8F8AA61",
            "identifier": "img_recognition",
            "msg": "",
            "sessionId": "285188972D40C8B2119C26D682DDFCF",
            "userId": "123321"
        },
        "process": [
            {
                "inputDatasets": [],
                "outputDatasets": ["words"],
                "action": "dataset_input_columns"
            }
        ]
    }

    # data_str = sys.argv[1][1:]
    # data_str数据形式为  b{"data":{},"header":{},"process":[{}]}
    # data = json.loads(data_str)
    Get_dataset_info(dataTest)
