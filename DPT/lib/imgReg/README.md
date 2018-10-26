## 自定义图像识别模型

### 使用方法

4. APIs
1. 上传图片
2. 训练模型
3. 加载使用模型

#### APIs

使用API训练模型：

必要输入参数：train_dir

返回字典：

`{'model_path': model_path, 'top1_accuracy': top1_accuracy, 'top5_accuracy': top5_accuracy}`

    from entrobus import image_recognition
    import json
    
    config = {
        'phase': 'train',
        'train_dir': "./data/train',
        'val_dir': "./data/val",
        'gpu': True
        ...
    }
    config = json.dumps(config)
    train_rlt = image_recognition.train(config)
    print(json.loads(train_rlt))
    
使用API测试图片

必要输入参数：phase, url, model_path

返回前五预测：

`{class1: prob1, class2: prob2 ...}`

    from entrobus import image_recognition
    import json
    
    config = {
        'phase': 'test',
        'url': "xxxx',
        'model_path': "xxx",
        ...
    }
    config = json.dumps(config)
    test_rlt = image_recognition.test(config)
    print(json.loads(test_rlt))


#### 上传图片

图片目录格式要求：

    |-- train_dir
        |-- class1
            |-- img1.jpg
            |-- img2.jp
            ...
        |-- class2
            |-- img1.jpg
            ...
        |-- class3
        ...

    
    

#### 训练模型

必要参数：

--train_dir 用户指定

默认参数：

--phase = "train"

--model_name = "squeezenet"

--epochs = 10

--batch_size = 8

--lr = 0.001

--momentum = 0.9

--gpu = False

--save_path = "./model.pt"

	./main.py --phase train --train_dir ./data/train --val_dir ./data/val \
        --epochs 10 --batch_size 4 \
        --lr 0.001 --momentum 0.9 \
        --gpu True --save_path ./model.pt


#### 加载使用模型

必要参数：

--url 图片url地址

--model_path 所使用的模型地址

    ./main.py --phase test --url xxx --model_path xxx
    
    

