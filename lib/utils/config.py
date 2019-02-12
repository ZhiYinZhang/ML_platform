#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/26 13:56

#--------------------------------------------------------rabbitmq的配置--------------------------------------------------
mq_user = "cloudai"
mq_passwd = "cloudai"
mq_host="139.199.161.144"
mq_port=5672
virtual_host="cloudai"
exchange_type="topic"
#消费rabbitmq的配置
consumer_exchange="data_process"
consumer_queueName="data_process"
consumer_routeKey="data_process"
#写回rabbitmq的配置
producer_exchange="data_result"
producer_queueName="data_result"
producer_routeKey="data_result"
content_type = "text/plain"


#-----------------------------------------------------------hdfs配置-----------------------------------------------------
#项目根目录
DPT_home = "/user/badisjob/DPT"
#用户目录
user_path = DPT_home+"/user"
#公共数据集目录
publicDataset_path = DPT_home+"/publicDataset"

#使用hdfs模块  写实验参数、读取实验的中间数据
hdfs_url = "http://entrobus28:50070"
hdfs_user = "badisjob"

#在实验中 spark读取数据源，写实验的中间数据
hdfs_host = "hdfs://entrobus28:8020"

#-----------------------------------------------------------livy配置-----------------------------------------------------
livy_host = "http://10.18.0.29:8998"
headers = {"Content-Type": "application/json"}
#livy创建每个session的资源大小
livy_param = {
    "executorMemory": "4G",
    "executorCores": 2,
    "numExecutors": 2,
}
init_session = 10
#----------------------------------------------------------用户中间文件过期配置--------------------------------------------
#游客数据过期设置，将游客整个目录删除，单位：s
tourist_expire = 1*60*60
#删除用户目录下过期的实验数据，单位：s
user_expire = 1*60*60