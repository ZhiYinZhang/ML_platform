# -*- coding: utf-8 -*-

import pika
import json
from lib.utils.config import *

#图片识别
data1={
        "data": {},
        "headers": {
            "code": 0,
            "identifier": "img_recognition",
            "msg": "",
            "expId": "EA96AF677276F9DA54865C8EA8F8AA61",
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
#读取数据集的信息
data2 = {
          "data": {
		       "expId": "F57CE7A920C54DFFB950A96CF5F55308"
	      },
	      "headers": {
		    "code": 0,
            "expId": "F57CE7A920C54DFFB950A96CF5F55308",
		    "identifier": "exp_execute_all",
		    "msg": "",
		    "sessionId": "92E58BBABA98BDB36DD7BBCE539EF91B",
		    "userId": ""
	      },
	      "process":[
          {
            "inputDatasets": [],
            "outputDatasets": ["model01_train.csv"],
            # "action": "dataset_input_columns"
            # "action": "dataset_columns"
            "action": "top_data"
            # "action" : "evaluate_data"
            # "action" : "terminate_app"
          }
    ]
}
data3 = {
	"data":
      {
	    "expId": "EA96AF677276F9DA54865C8EA8F8AA61"
	  },
	"headers":
	  {
	    "code": 0,
		"expId": "EA96AF677276F9DA54865C8EA8F8AA61",
		"identifier": "exp_execute_all",
		"msg": "",
		"sessionId": "285188972D40C8B2119C26D682DDFCF",
		"userId": "123321"
	  },
	"process":
	  [
	    {
		  "inputDatasets" :[],
		  "outputDatasets" :["model08_train.csv"],
		  "action" : "dataset",
		  "filePath" : "",
          # "end_execute_node" : "1",
		  "fileFormat" : "csv"
		},
	    {
		  "inputDatasets": ["model08_train.csv"],
		  "outputDatasets": ["split_01_Output_01","split_01_Output_02"],
		  "action": "split",
		  "weight":[0.7,0.3],
		  "outLeft":"split_01_Output_01",
		  "outRight":"split_01_Output_02",
		  "IDColName" : "index"
		}
	  ]
}
def send_msg():
    credential = pika.PlainCredentials(mq_user, mq_passwd)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_host,
                                                                   credentials=credential,
                                                                   virtual_host=virtual_host,
                                                                   port=mq_port
                                                                   ))
    channel = connection.channel()
    channel.exchange_declare(exchange=producer_exchange,
                             exchange_type=exchange_type,
                             durable=True
                             )
    channel.queue_declare(queue=producer_queueName)
    # 绑定
    channel.queue_bind(exchange=producer_exchange,
                       queue=producer_queueName,
                       routing_key=producer_routeKey)

    channel.basic_publish(exchange=producer_exchange,
                          routing_key=producer_routeKey,
                          body=json.dumps(data))

    print("message sent")

    connection.close()


if __name__ == '__main__':
    #模板
    path = "../../files/model06.json"
    with open(path, 'r') as f:
        data0 = json.load(f)
    data = data2
    send_msg()

