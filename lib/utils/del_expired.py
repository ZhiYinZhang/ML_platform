#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/22 18:05
from lib.utils.hdfsClient import *
import time
import re
from lib.utils.config import *

def del_expire_data(header:dict):
      cli = get_hdfs_Client()
      userId = header['userId']

      path = f"{user_path}/{userId}"

      # 当前时间：s
      curr_time = time.time()
      #过期时间 1小时
      expire_time = user_expire
      #删除用户目录下的过期的试验数据
      for p in cli.list(path):

          dir_info = cli.status(hdfs_path=f"{path}/{p}")
          #文件最近一次修改时间：s
          last_modify_time = dir_info['modificationTime']/1000
          #12小时
          if (curr_time-last_modify_time)>=expire_time:
              cli.delete(hdfs_path=f"{path}/{p}",recursive=True)
              print(f"删除{userId}用户的过期实验数据{p}")




def del_tourist():
    cli = get_hdfs_Client()
    path = user_path
    # 当前时间：s
    curr_time = time.time()
    #过期时间
    expire_time = tourist_expire
    # 删除游客目录
    for name in cli.list(path):
        if re.findall(r'^(Tourist)', name):
            dir_info = cli.status(hdfs_path=f"{path}/{name}")

            last_modify_time = dir_info['modificationTime'] / 1000

            if (curr_time - last_modify_time) >= expire_time:
                cli.delete(hdfs_path=f"{path}/{name}", recursive=True)
                print(f"删除游客{name}用户")

if __name__ == "__main__":
    header={
        "code": 0,
        "expId": "E3204CA4A11B1F72E3768CC314190150",
        "identifier": "exp_execute_all",
        "msg": "",
        "sessionId": "737592BFDA77CA7C55F3C7BE65655D4C",
        "userId": "0D70A81015BC54C9E17FD19DD5EEF713"
    }
    del_expire_data(header)
