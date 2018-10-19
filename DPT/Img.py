#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/28 10:29
from lib.utils import write2mq
from lib import buffet
import sys
import time
import json
import traceback as tb
def img_recognition(data_args):
    print(f"消息{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{data_args}")
    #获取图片url
    url=data_args['process'][0]['url']
    result = []
    try:
        #识别的结果
        img=json.loads(buffet.buffet_infer(url=url))
        for i in img:
            img_dict = {"score": '%.6f' % i[1], "keyword": i[0]}
            result.append(img_dict)
        data_args['process'][0]['code'] = 0
        data_args['process'][0]['msg'] = 'success'
    except Exception as e:
        exc_info = tb.format_exc()
        print(exc_info)
        data_args['process'][0]['code'] = 1
        data_args['process'][0]['msg'] = exc_info.split("\n")[-2]

    data['process'][0]['result'] = result
    write2mq.write(data_args)
    print(f"结果{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{data_args}")


if __name__=="__main__":
    #调试
    dataTest = {
        "data": {},
        "headers": {
            "code": 0,
            "identifier": "img_recognition",
            "msg": "",
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
    data_str = sys.argv[1][1:]
    # data_str = dataTest
    #data_str数据形式为  b{"data":{},"header":{},"process":[{}]}
    data = json.loads(data_str)
    img_recognition(data)