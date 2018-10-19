#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/11 16:07
from hdfs import client

def get_hdfs_Client():
       #root为根目录
       return client.InsecureClient(url="http://entrobus11:50070",user="zhangzy",root="/user/zhangzy/DPT")
