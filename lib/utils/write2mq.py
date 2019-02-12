#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pika
import json
from lib.utils.config import *
# 写回RabbitMQ
def write(res_data: dict):

    res_content = json.dumps(res_data)
    # TODO RabbitMQ配置被写死，将其写入配置文件中
    credential = pika.PlainCredentials(mq_user,mq_passwd)

    # 连接RabbitMQ
    with pika.BlockingConnection(pika.ConnectionParameters(host=mq_host,
                                                           credentials=credential,
                                                           virtual_host=virtual_host,
                                                           port=mq_port
                                                           )) as connection:
        # 定义channel
        channel = connection.channel()
        # declare exchange
        channel.exchange_declare(exchange=producer_exchange,
                                 exchange_type=exchange_type,
                                 durable=True
                                 )
        #declare queue
        channel.queue_declare(queue=producer_queueName)

        # 绑定
        channel.queue_bind(exchange=producer_exchange,
                           queue=producer_queueName,
                           routing_key=producer_routeKey)
        msg_props = pika.BasicProperties()
        msg_props.content_type = content_type
        # publish message
        channel.basic_publish(exchange=producer_exchange,
                              routing_key=producer_routeKey,
                              properties=msg_props,
                              body=res_content)
        connection.close()