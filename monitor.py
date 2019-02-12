#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/27 15:01
import subprocess as sp
import time
from onLivy import *
from dpt import *
while True:
        time.sleep(3)
        try:
            status_consumer()
        except:
            start_consumer()
        try:
          sp.check_output("ps -ef|grep livy|grep -v grep")
        except:
            start_livy()
        try:
            status_monitor()
        except:
            start_monitor()
        #重启consumer
        if time.localtime().tm_hour ==3:
            stop_consumer()
            start_consumer()

        livy_sessions = get_sessions()
        num = len(livy_sessions['busy'])+len(livy_sessions['idle'])
        if num < init_session:
            for i in range(init_session-num):
                 create(param=livy_param)