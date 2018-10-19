# -*- coding: utf-8 -*-

import pika
import json


data1 = {
    "process": [
        {
            "input": [0],
            "output": [1],
            "operate": "readHdfs",
            "filePath": "files/sample_libsvm_data.txt",
            "fileFormat": "libsvm"
        },
        {
            "input": [1],
            "output": [2],
            "operate": "stringIndex",
            "inputCol": "label",
            "outputCol": "indexedLabel"
        },
        {
            "input": [2],
            "output": [3],
            "operate": "vectorIndex",
            "inputCol": "features",
            "outputCol": "indexedFeatures",
            "maxCategories": 4
        },
        {
            "input": [3],
            "output": [4, 5],
            "operate": "split",
            "weight": [0.7, 0.3],
            "outLeft": 4,
            "outRight": 5
        },

        {
            "input": [4],
            "output": [6],
            "operate": "train",
            "model_name": "GBDT",
            "labelCol": "indexedLabel",
            "featuresCol": "indexedFeatures",
            "maxIter": 10
        },
        {
            "input": [5, 6],
            "output": [7],
            "operate": "predict",
            "model": 6,
            "testSet": 5
        },
        {
            "input": [7],
            "output": [8],
            "operate": "evaluate",
            "labelCol": "indexedLabel",
            "predictionCol": "prediction",
            "metricName": "accuracy"
        }
    ]
}
#图片识别
data2={
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
data3 = {
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
	      "process":[
          {
            "inputDatasets": [],
            "outputDatasets": ["model04_train.csv"],
            "action": "dataset_input_columns"
            # "action": "dataset_columns"
            # "action": "top_data"
          }
    ]
}
data4 = {
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



#rabbitmq的配置
user = "cloudai"
passwd = "cloudai"
host="139.199.161.144"
port=5672
virtual_host="cloudai"
# exchange="pub_data_clean"
# queue_name="pub_data_clean"
# route_key="data_clean"
exchange="data_process"
queue_name="data_process"
route_key="data_process"
def send_msg():
    credential = pika.PlainCredentials(user, passwd)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                                   credentials=credential,
                                                                   virtual_host=virtual_host,
                                                                   port=port
                                                                   ))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange,
                             exchange_type="topic",
                             durable=True
                             )
    channel.queue_declare(queue=queue_name)
    # 绑定
    channel.queue_bind(exchange=exchange,
                       queue=queue_name,
                       routing_key=route_key)

    channel.basic_publish(exchange=exchange,
                          routing_key=route_key,
                          body=json.dumps(data))

    print("message sent")

    connection.close()


if __name__ == '__main__':
    path = "e:/pythonProject/DPT/files/123321/EA96AF677276F9DA54865C8EA8F8AA61/test.json"
    with open(path, 'r') as f:
        data0 = json.load(f)
    data = data0
    send_msg()
