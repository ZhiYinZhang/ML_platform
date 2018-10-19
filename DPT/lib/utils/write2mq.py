#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pika
import json

# 写回RabbitMQ
def write(res_data: dict):
    # rabbitmq的配置
    user = "cloudai"
    passwd = "cloudai"
    host = "139.199.161.144"
    port = 5672
    virtual_host = "cloudai"
    # exchange = "sub_clean_result"
    # route_key="data_clean"
    # queue_name="sub_clean_result"
    exchange="data_result"
    queue_name="data_result"
    route_key="data_result"

    res_content = json.dumps(res_data)
    # TODO RabbitMQ配置被写死，将其写入配置文件中
    credential = pika.PlainCredentials(user,passwd)

    # 连接RabbitMQ
    with pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                           credentials=credential,
                                                           virtual_host=virtual_host,
                                                           port=port
                                                           )) as connection:
        # 定义channel
        channel = connection.channel()
        # declare exchange
        channel.exchange_declare(exchange=exchange,
                                 exchange_type="topic",
                                 durable=True
                                 )
        #declare queue
        channel.queue_declare(queue=queue_name)

        # 绑定
        channel.queue_bind(exchange=exchange,
                           queue=queue_name,
                           routing_key=route_key)
        msg_props = pika.BasicProperties()
        msg_props.content_type = "text/plain"
        # publish message
        channel.basic_publish(exchange=exchange,
                              routing_key=route_key,
                              properties=msg_props,
                              body=res_content)
        connection.close()