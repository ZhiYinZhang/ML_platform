#!/user/env/bin python

# -*- coding: utf-8 -*-
from lib.utils import parser,write2mq,hdfsClient
import json
import os
import time
import pika
import Img
import subprocess as sp
import traceback as tb
from lib.NameToHandler import *
from lib.utils.config import *
# 收到消息后的回调函数
# TODO 更改消费模式，消费任务后返回ack，确保消息不丢失
def callback(ch, method, properties, body):
    # 解析收到的消息
    data = json.loads(body)
    #图片识别
    if data['process'][0]['action']=="img_recognition":
        s=f"python3 Img.py {body}"
        sp.Popen(s,shell=True)
        # Img.img_recognition(data)
    elif data['process'][0]['action']=='terminate_app':
        s = f"python3 lib/utils/terminate_application.py {body}"
        sp.Popen(s,shell=True)
    #获取数据集信息 的操作
    elif data['process'][0]['action'] in dataset_name_to_handler.keys():
        s = f"python3 DatasetInfo.py {body}"
        sp.Popen(s,shell=True)
        #DatasetInfo.Get_dataset_info(data)
    else:
       try:
            res = parser(data)
            # 获取user id，session id， 文件路径
            user_id = res["headers"]["userId"]
            experiment_id = res["headers"]["expId"]
            session_id = res["headers"]["sessionId"]         
            if user_id=="":
                     user_id=f"Tourist_{experiment_id}"
                     res["headers"]["userId"] = user_id

             # 解析后的DAG图存放目录
            dir_path = f"{user_path}/{user_id}/{experiment_id}"

            cli = hdfsClient.get_hdfs_Client()
             # 判断目录是否存在
            if not cli.status(hdfs_path=dir_path, strict=False):
                 cli.makedirs(hdfs_path=dir_path)
            # 写入文件参数
            hdfs_path = f"{dir_path}/{session_id}.json"
            with cli.write(hdfs_path=hdfs_path, encoding='utf-8') as writer:
                 json.dump(res, writer)

            # --name 终止应用时使用
            spark_submit: str = f"spark2-submit --name {experiment_id} " \
                                "--num-executors 4 " \
                                "--executor-cores 4 " \
                                "--executor-memory 5G " \
                                "--total-executor-cores 16 " \
                                f"--files {hdfs_host}{hdfs_path}#data.json "\
                                "--master yarn --deploy-mode cluster " \
                                "--py-files lib.zip CoreApp.py"

            # TODO 异步提交
            sp.Popen(f"python3 onLivy.py {hdfs_path}", shell=True)
            # sp.Popen(spark_submit,shell=True)
       except Exception as e:
           tb.print_exc()
    

def consume():
    # 配置消费者
    # TODO 更改配置方式，写入配置文件
    credential = pika.PlainCredentials(mq_user, mq_passwd)
    # 创建RabbitMQ连接
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_host,
                                                                   credentials=credential,
                                                                   virtual_host=virtual_host,
                                                                   port=mq_port
                                                                   ))
    # 初始化channel
    channel = connection.channel()
    channel.exchange_declare(exchange=consumer_exchange,
                             exchange_type=exchange_type,
                             durable=True)
    channel.queue_declare(queue=consumer_queueName)
    # 绑定
    channel.queue_bind(exchange=consumer_exchange,
                       queue=consumer_queueName,
                       routing_key=consumer_routeKey)

    channel.basic_consume(consumer_callback=callback,
                          queue=consumer_queueName,
                          no_ack=True)
    channel.start_consuming()


if __name__ == '__main__':
    dataTest = {
        "data": {},
        "headers": {
            "code": 0,
            "identifier": "img_recognition",
            "msg": "",
            "sessionId": "ED14A9BB67156327716F44C6409B2540",
            "userId": ""
        },
        "process": [
            {
                "labelColName": [
                ],
                "seed": 0,
                "end_execute_node": "1",
                "instanceType": 2,
                "stepSize": 0.1,
                "minInfoGain": 0,
                "outputDatasets": [
                    "GBDT_model_1_Output_1"
                ],
                "minInstancesPerNode": 1,
                "featureColNames": [

                ],
                "maxDepth": 10,
                "subsamplingRate": 1,
                "instanceId": "2C874ADCEE1B51B85DE4D64CCF7534B5",
                "maxBins": 32,
                "maxIter": 20,
                "action": "GBDT_model",
                "inputDatasets": [
                    "model04_train.csv"
                ]
            },
        ]
    }
    i = 1
    while True:
          try:
            consume()
          except:
            tb.print_exc()
          print(f"第 {i} 次重新连接...")
          time.sleep(3)
          i += 1