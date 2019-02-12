#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/29 17:33
import subprocess as sp
import sys
import json

def terminate_app(header: dict):
    expId = header['expId']
    # 获取正在运行的spark应用
    command = "yarn application -list"
    result = sp.check_output(command, shell=True)

    result = result.__str__()[2:-1]
    result = result.split("\\n")

    # apps = []
    for i in result[2:-1]:
        param = []
        for j in i.split('\\t'):
            param.append(j.strip(' '))
        if param[1] == expId:
            command = f"yarn application -kill {param[0]}"
            sp.Popen(command,shell=True)
        # #用户'zhangzy'
        # if param[3] == "zhangzy":
        #     d = {"Application-Id": param[0],
        #          "Application-Name": param[1],
        #          "Application-Type": param[2],
        #          "User": param[3],
        #          "Queue": param[4],
        #          "State": param[5],
        #          "Final-State": param[6],
        #          "Progress": param[7],
        #          "Tracking-URL": param[8]}
        #     apps.append(d)

if __name__=="__main__":
    # 数据形式为  b{"data":{...}}
    # data_str = sys.argv[1][1:]
    # data = json.loads(data_str)
    # header = data['headers']
    # terminate_app(header=header)
    pass