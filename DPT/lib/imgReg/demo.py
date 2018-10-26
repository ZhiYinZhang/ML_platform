#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/25 18:11
from entrobus import image_recognition

import json

if __name__ == "__main__":
    # config = {
    #     'phase': 'train',
    #     'train_dir': r"E:\pythonProject\dataset\hymenoptera_data\train",
    #     'val_dir': r"E:\pythonProject\dataset\hymenoptera_data\val",
    #     'gpu': False,
    #     'epochs':1
    # }
    #
    # config = json.dumps(config)
    # train_rlt = image_recognition.train(config)
    #
    # result = json.loads(train_rlt)
    # print(result)

    config = {
        'phase':'test',
        'url': 'https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1540527267206&di=600116cec3c04f14d38e6bc36afb3921&imgtype=0&src=http%3A%2F%2Fd.ifengimg.com%2Fw128%2Fp0.ifengimg.com%2Fpmop%2F2017%2F0630%2F5987333F023402291A096950D5B177684D9A5DC2_size290_w600_h350.gif',
        'model_path':'./model_default.pt'
    }

    config = json.dumps(config)
    test_rlt = image_recognition.test(config)
    print(json.loads(test_rlt))