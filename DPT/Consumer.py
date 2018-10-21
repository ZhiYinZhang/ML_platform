#!/home/hadoop/anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
  从RabbitMQ中接受数据
  根据数据需要执行的操作：
    操作分为三种
      图片识别:python程序
      查看中间数据集信息:python读取hdfs
      机器学习:使用pyspark

"""

from lib.utils import parser,write2mq
import json
import traceback as tb
import os
import pika
import Img,DatasetInfo
import subprocess as sp
import time

# 收到消息后的回调函数

# TODO 更改消费模式，消费任务后返回ack，确保消息不丢失
def callback(ch, method, properties, body):
    # 解析收到的消息 body为bytes
    data = json.loads(body)
    #存储 实验id : 进程对象
    childs={}
    #图片识别
    if data['process'][0]['action']=="img_recognition":
        s=f"python3 Img.py {body}"
        # sp.Popen(s,shell=True)
        Img.img_recognition(data)
    # elif data['process'][0]['action']=='exit':
    #判断子进程死了之后 主进程怎么获取状态

    #查看数据集信息
    elif len(data['process']) ==1:
        s = f"python3 DatasetInfo.py {body}"
        # sp.Popen(s,shell=True)
        DatasetInfo.Get_dataset_info(data)
    #执行DAG
    else:
        try:
            res = parser(data)
            # 获取user id，session id，exp id 文件路径
            session_id = res["headers"]["sessionId"]
            experiment_id = res["headers"]["expId"]
            user_id = res["headers"]["userId"]
            if user_id == "":
                  user_id=f"Tourist_{experiment_id}"
                  res["headers"]["userId"] = user_id
            dir_path = os.path.join("files/", user_id, experiment_id)

            # 如果work文件夹不存在，创建work文件夹
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            # 写入文件参数
            with open(os.path.join(dir_path, session_id + ".json"), "w") as f:
                f.write(json.dumps(res))

            spark_submit: str = f"spark2-submit --num-executors 4 " \
                                "--executor-cores 4 " \
                                "--executor-memory 5G " \
                                "--total-executor-cores 16 " \
                                f"--files files/{user_id}/{experiment_id}/{session_id}.json#data.json " \
                                "--master yarn --deploy-mode cluster " \
                                "--py-files lib.zip CoreApp.py"

            # TODO 异步提交
            # child=sp.Popen(spark_submit,shell=True)
            # childs[experiment_id] = child
        except Exception:
            tb.print_exc()







#rabbitmq的配置
user = "cloudai"
passwd = "cloudai"
host="139.199.161.144"
port=5672
virtual_host="cloudai"
exchange="pub_data_clean"
queue_name="pub_data_clean"
route_key="data_clean"
# exchange="data_process"
# queue_name="data_process"
# route_key="data_process"
def consume():
    # 配置消费者
    # TODO 更改配置方式，写入配置文件
    credential = pika.PlainCredentials(user, passwd)
    # 创建RabbitMQ连接
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                                   credentials=credential,
                                                                   virtual_host=virtual_host,
                                                                   port=port
                                                                   ))
    # 初始化channel
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange,
                             exchange_type="topic",
                             durable=True)
    channel.queue_declare(queue=queue_name)
    # 绑定
    channel.queue_bind(exchange=exchange,
                       queue=queue_name,
                       routing_key=route_key)

    channel.basic_consume(consumer_callback=callback,
                          queue=queue_name,
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
                "action": "img_recognition",
                "inputDatasets": [],
                "outputDatasets": [],
                "url": "https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1538054725399&di=454f9c38fa3655821603641d3ebaae9d&imgtype=0&src=http%3A%2F%2Fimage.woshipm.com%2Fwp-files%2F2015%2F07%2Fyestone_HD_1112835095.jpg"
            }
        ]
    }
    consume()
    # img_recognition(dataTest)
