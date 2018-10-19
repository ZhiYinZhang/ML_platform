# -*- coding: utf-8 -*-
import json


class DagParser:
    false = False
    true = True

    @classmethod
    def operation_format(cls, operation: dict):
        """
        reformat training model
        "action" : "train"
        "arges" :  {
            "parameter" : "value",
            ...
            }
        :param operation:
        :return:
        """

        if operation["action"].endswith("_model"):
            operation["model_name"] = operation["action"]
            operation["action"] = "train"

        return operation

    @classmethod
    def process_format(cls,process: list):
        flag = False
        # 判断是否有'end_execute_node'
        for key in process:
            if key.get('end_execute_node'):
                # 有设为True
                flag = True
        if flag:
            # end_execute_node之前的所有操作，包括自己
            front_all = []
            for key in process:
                front_all.append(key)
                if key.get('end_execute_node'):
                    break

            single_line = []
            # 将标为'end_execute_node'的操作添加到single_line
            single_line.append(front_all[-1])
            # d1.remove(d1[-1])
            for i in single_line:
                # 遍历操作的inputDatasets
                for input in i["inputDatasets"]:
                    # 遍历d1
                    for action in front_all:
                        if input in action['outputDatasets']:
                            # print(action)
                            single_line.append(action)
                            front_all.remove(action)

            single_line.reverse()
            for i in single_line:
                process.remove(i)
            single_line.extend(process)
            process = single_line
        return process


    @classmethod
    def parse(cls, data: dict) -> dict:
        """
        将前端的json包进行解析
        将打乱的DAG图排序，返回正确的工作序列
        :param data:
        :return: 排序后的工作流
        """
        operations: list = data["process"]
        # 作为过input的元素
        as_input = set()
        # 作为过output的元素
        as_output = set()
        # 操作队列
        ope_list = []

        for operation in operations:
            for in_elem_name in operation["inputDatasets"]:
                as_input.add(in_elem_name)
            for out_elem_name in operation["outputDatasets"]:
                as_output.add(out_elem_name)

        # 输入及输出元素
        input_elms = as_input - as_output
        output_elms = as_output - as_input
        output_store = output_elms.copy()

        # 正向遍历，按处理逻辑选择执行顺序
        while len(output_elms) is not 0:
            # 判断本次循环是否有操作被选择
            changed = False
            for operation in operations:
                ready = True
                for in_elem_name in operation["inputDatasets"]:
                    if in_elem_name not in input_elms:
                        ready = False
                        break

                # 输入元素已就绪，可以执行
                if ready:
                    changed = True
                    # 将该operation push入队列
                    operation_fm = cls.operation_format(operation)
                    # ope_list.append(operation)
                    ope_list.append(operation_fm)

                    # 对于本次operation中的每个output来说
                    # 如果是最终的输出元素，直接将其从output_elms中删除即可
                    # 否则肯定为某个操作的输出，将其插入input_elms中
                    for out_elem_name in operation["outputDatasets"]:
                        if out_elem_name in output_elms:
                            output_elms.remove(out_elem_name)
                        else:
                            input_elms.add(out_elem_name)

                    # 将该operation从待选列表删除，防止重复遍历
                    operations.remove(operation)

            # 输入数据有误
            # 本次循环中没有加入任何元素，将陷入死循环
            if not changed:
                print("Incorrect input data")
                exit(1)

        ope_list = cls.process_format(ope_list)

        res_data = {
            "data":data["data"],
            "headers":data["headers"],
            "process": ope_list,
            "outElem": list(output_store)
            # "sessionId": data["sessionId"],
            # "userId": str(data["userId"]),

        }

        return res_data



if __name__=="__main__":
    d={
        "data":{
            "expId":"470452BE2A31E750E8A41A177C46FB1E"
        },
        "headers":{
            "code":0,
            "expId":"470452BE2A31E750E8A41A177C46FB1E",
            "identifier":"exp_execute_all",
            "msg":"",
            "sessionId":"82C3408E1CC06DFA4142E200FDDA214A",
            "userId":""
        },
        "process":[
            {
                "instanceId":"403C9D03846A0776F59D17856A978731",
                "instanceType":1,
                "action":"dataset",
                "inputDatasets":[

                ],
                "outputDatasets":[
                    "model04_train.csv"
                ]
            },
            {
                "labelColName":[

                ],
                "seed":0,
                "end_execute_node":"1",
                "instanceType":2,
                "stepSize":0.1,
                "minInfoGain":0,
                "outputDatasets":[
                    "GBDT_model_1_Output_1"
                ],
                "minInstancesPerNode":1,
                "featureColNames":[

                ],
                "maxDepth":10,
                "subsamplingRate":1,
                "instanceId":"2C874ADCEE1B51B85DE4D64CCF7534B5",
                "maxBins":32,
                "maxIter":20,
                "action":"GBDT_model",
                "inputDatasets":[
                    "sampling_1_Output_1"
                ]
            },
            {
                "labelColName":[

                ],
                "tol":0.001,
                "instanceId":"57082A85B1B2AA299125639001A5B392",
                "aggregationDepth":2,
                "maxIter":100,
                "instanceType":2,
                "action":"SVM_model",
                "inputDatasets":[
                    "sampling_3_Output_1"
                ],
                "regParam":0,
                "outputDatasets":[
                    "SVM_model_1_Output_1"
                ],
                "featureColNames":[

                ]
            },
            {
                "instanceId":"5B712B86FD200D873523F0EA59C025F6",
                "withReplacement": False,
                "instanceType":2,
                "action":"sampling",
                "inputDatasets":[
                    "sampling_3_Output_1"
                ],
                "outputDatasets":[
                    "sampling_4_Output_1"
                ],
                "fraction":0.1
            },
            {
                "instanceId":"73D39DDF6018777E73FE9278A39FC080",
                "instanceType":2,
                "action":"predict",
                "inputDatasets":[
                    "sampling_4_Output_1",
                    "SVM_model_1_Output_1"
                ],
                "outputDatasets":[
                    "predict_2_Output_1"
                ],
                "featureColNames":[

                ]
            },
            {
                "instanceId":"75662B9AC9843A5C15B0CFD3D94A1CB4",
                "withReplacement":False,
                "instanceType":2,
                "action":"sampling",
                "inputDatasets":[
                    "typetransform_1_Output_1"
                ],
                "outputDatasets":[
                    "sampling_1_Output_1"
                ],
                "fraction":0.1
            },
            {
                "instanceId":"7744FFCB8012963C0C84CBBEB55C3376",
                "instanceType":2,
                "action":"predict",
                "inputDatasets":[
                    "GBDT_model_1_Output_1",
                    "sampling_2_Output_1"
                ],
                "outputDatasets":[
                    "predict_1_Output_1"
                ],
                "featureColNames":[

                ]
            },
            {
                "instanceId":"7EC259E2D5169DA4CFF213612A75A8B6",
                "withReplacement": False,
                "instanceType":2,
                "action":"sampling",
                "inputDatasets":[
                    "typetransform_1_Output_1"
                ],
                "outputDatasets":[
                    "sampling_3_Output_1"
                ],
                "fraction":0.1
            },
            {
                "instanceId":"8C43FDB127565B30E1C15AED2EED4A1A",
                "withReplacement": False,
                "instanceType":2,
                "action":"sampling",
                "inputDatasets":[
                    "sampling_1_Output_1"
                ],
                "outputDatasets":[
                    "sampling_2_Output_1"
                ],
                "fraction":0.1
            },
            {
                "defaultStringVal":"",
                "instanceId":"99498B2A68901750697917CAD40C0FB5",
                "selectedColtoString":[

                ],
                "defaultIntVal":0,
                "instanceType":2,
                "defaultDoubleVal":0,
                "selectedColtoInt":[

                ],
                "action":"typetransform",
                "selectedColtoDouble":[
                    {
                        "field":"Age",
                        "fieldType":"double"
                    },
                    {
                        "field":"Fare",
                        "fieldType":"double"
                    },
                    {
                        "field":"Survived",
                        "fieldType":"double"
                    },
                    {
                        "field":"Deck_A",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_B",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_C",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_D",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_E",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_F",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_G",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_T",
                        "fieldType":"int"
                    },
                    {
                        "field":"Deck_U",
                        "fieldType":"int"
                    },
                    {
                        "field":"Embarked_C",
                        "fieldType":"int"
                    },
                    {
                        "field":"Embarked_Q",
                        "fieldType":"int"
                    },
                    {
                        "field":"Embarked_S",
                        "fieldType":"int"
                    },
                    {
                        "field":"Family_label",
                        "fieldType":"int"
                    },
                    {
                        "field":"Parch",
                        "fieldType":"int"
                    },
                    {
                        "field":"Pclass",
                        "fieldType":"int"
                    },
                    {
                        "field":"Sex_female",
                        "fieldType":"int"
                    },
                    {
                        "field":"Sex_male",
                        "fieldType":"int"
                    },
                    {
                        "field":"SibSp",
                        "fieldType":"int"
                    },
                    {
                        "field":"Title_Master",
                        "fieldType":"int"
                    },
                    {
                        "field":"Title_Miss",
                        "fieldType":"int"
                    },
                    {
                        "field":"Title_Mr",
                        "fieldType":"int"
                    },
                    {
                        "field":"Title_Mrs",
                        "fieldType":"int"
                    },
                    {
                        "field":"Title_Officer",
                        "fieldType":"int"
                    },
                    {
                        "field":"Title_Royalty",
                        "fieldType":"int"
                    },
                    {
                        "field":"ticket_label",
                        "fieldType":"int"
                    }
                ],
                "inputDatasets":[
                    "model04_train.csv"
                ],
                "outputDatasets":[
                    "typetransform_1_Output_1"
                ]
            }
        ]
    }

    result=DagParser.parse(d)
    with open("e://pythonProject//DPT//files//123321//EA96AF677276F9DA54865C8EA8F8AA61//test.json",'w') as file:
        json.dump(result,file)
