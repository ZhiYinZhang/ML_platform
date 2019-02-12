#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/11 16:07
from hdfs import client
from lib.utils.config import *
def get_hdfs_Client():
       #url为namenode
       return client.InsecureClient(url=hdfs_url,user=hdfs_user)