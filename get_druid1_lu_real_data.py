import requests
import json
import datetime
import sys
import time
from time import sleep
from config import *

#!!!!!!,临时借用基线组的数据，正式实验生效的时候需要先确认德鲁伊里面有实验组的数据，然后将base-value改成实验组
target_str = "PID_ALPHA_PST_AB_V2d3"
base_value = "PID_ALPHA_PST_AB_V1"

def get_pst_landingpage_spent(pstIdLists, min_cost): #只用在process3函数
    cur_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())) + ":00"
    if datetime.datetime.now().hour == 0 and datetime.datetime.now().minute < COLD_START_MINUTES:
        #today_start = (datetime.datetime.now() - datetime.timedelta(days=0, minutes = COLD_START_MINUTES)).strftime('%Y-%m-%d %H:%M:%S')
        today_start = str(datetime.date.today()) + " 00:00:00"
    else:
        today_start = str(datetime.date.today()) + " 00:00:00"
    print("druid1 start_time: %s"%today_start)
    print("druid1 end time: %s"%cur_time)
    url = 'http://api.dataservice.prd.vivo.lan:8080/api/data/query'
    headers = {'Content-Type':'application/json'}
    res = dict({})
    total_cost = 0
    total_income = 0
    for i in range(0, len(pstIdLists), 500):
        tmp_pst_list = pstIdLists[i: i+500]
        data = {
        "appKey":"lu_real_effect_data",
        "appSecret":"8tkNQCiWUBi7H82G",
        "queryId":1360,
        "startTime":today_start,
        "endTime":cur_time,
        "pst_uuid": tmp_pst_list,
        "min_cost": min_cost,
        }
        data_json = json.dumps(data)
        try:
            r = requests.post(url=url,headers=headers,data=data_json)
            r_new = json.loads(r.text)
            res_list = r_new['data']
        except:
            try_cnt = 0
            while try_cnt < 5:
                sleep(QPS_TIME)
                print("retry druid1: %d"%try_cnt)
                try:
                    r = requests.post(url=url,headers=headers,data=data_json)
                    print(r)
                    r_new = json.loads(r.text)
                    res_list = r_new['data']
                    break
                except:
                    try_cnt += 1
        if res_list is None:
            continue
        for log_json in res_list:
            lu_storeDsp_type = "1" if log_json['lu_storeDsp_type']=="1" else "0"
            cost = log_json['lu_cost']
            income = log_json['lu_income']
            pst_uuid = log_json['pst_uuid']
            media_uuid = log_json['media_uuid']
            total_cost += cost
            total_income += income
            res[media_uuid + "_" + pst_uuid + "_" + lu_storeDsp_type] = [cost, income]
        sleep(QPS_TIME)
    print("total_cost:", total_cost)
    print("total_income:", total_income)
    return res

#!!!,???,这个函数针对不同的德鲁伊id设置不同的参数
#只用在get_exp_pst_landingpage_spent函数中
def get_druid1_param(druid1_id, start_time, end_time, pst_list=None, min_cost=0, exps=None, query_list=None, media_or_pst="media"): 
    exp_types = [expinfo.exp_name for expinfo in exps]
    exp_types = [base_value if item == target_str else item for item in exp_types]
    base_data = {
            "appKey":"lu_real_effect_data",
            "appSecret":"8tkNQCiWUBi7H82G",
            "queryId":druid1_id
            }
    if druid1_id == 1368: #!!!,具体参数如何设置，参考https://dataservice.vmic.xyz/page/dataset的id的sql代码
        base_data.update({
                "startTime": start_time,
                "endTime": end_time,
                "pst_uuid": pst_list,
                "min_cost": min_cost,
                "alpha_exp": exp_types
                })
    elif druid1_id == 1614:
        base_data.update({
                "startTime": start_time,
                "endTime": end_time,
                "min_cost": min_cost,
                "alpha_exp": exp_types,
                "media_or_pst": media_or_pst,
                "except_pst_uuid": pst_list,
                })
    elif druid1_id == 1369:
        base_data.update({
                "startTime": start_time,
                "endTime": end_time,
                "min_cost": min_cost,
                "alpha_exp": exp_types
                })
    elif druid1_id == 1377:
        base_data.update({
                "startTime": start_time,
                "endTime": end_time,
                "min_cost": min_cost,
                "alpha_exp": exp_types
                })
    elif druid1_id == 1414:
        base_data.update({
                "startTime": start_time,
                "endTime": end_time,
                "min_cost": min_cost
                })
    elif druid1_id == 1421:
        base_data.update({
                "startTime": start_time,
                "endTime": end_time,
                "min_cost": min_cost,
                "alpha_exp": exp_types
                })
    elif druid1_id == 1423 or druid1_id == 1425:
        base_data.update({
                "startTime": start_time,
                "endTime": end_time,
                "min_cost": min_cost
                })
    else:
        pass
    return base_data

#向Druid数据查询接口发送POST请求获取数据
#首先调用get_druid1_param设置参数
#只用在get_exp_pst_landingpage_spent函数中
def get_druid1_data(data_param):
    """
    向Druid数据查询接口发送POST请求获取数据，请求失败时进行最多5次重试
    :param data_param: dict，Druid查询参数（包含查询条件、维度、指标等）
    :return: list/dict/None，接口返回的data字段数据，失败时返回None
    """
    url = 'http://api.dataservice.prd.vivo.lan:8080/api/data/query'
    headers = {'Content-Type':'application/json'}
    data_json = json.dumps(data_param) #将查询参数字典序列化为JSON字符串，用于POST请求的请求体
    print(data_json)
    res_list = None
    try:
        r = requests.post(url=url,headers=headers,data=data_json) # 发送POST请求到Druid接口，传递请求头和JSON格式的请求体
        r_new = json.loads(r.text) # 将响应的文本内容解析为字典格式
        res_list = r_new['data'] # 提取响应字典中的data字段（核心查询数据）赋值给返回结果
    except:
        try_cnt = 0
        while try_cnt < 5:
            sleep(QPS_TIME)
            print("retry druid1: %d"%try_cnt)
            try:
                r = requests.post(url=url,headers=headers,data=data_json)
                print(r)
                r_new = json.loads(r.text)
                res_list = r_new['data']
                break
            except:
                try_cnt += 1
    return res_list # 返回最终的查询结果（成功为data数据，失败为None）

def get_exp_pst_landingpage_spent(exps, query_list=None, pst_id_list=None, min_cost=0): #只用在process4函数
    cur_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())) + ":00" #获取当前系统的本地时间，并格式化为年-月-日 时:分:00的字符串，如2025-12-19 15:30:00
    if datetime.datetime.now().hour == 0 and datetime.datetime.now().minute < COLD_START_MINUTES:
        #today_start = (datetime.datetime.now() - datetime.timedelta(days=0, minutes = COLD_START_MINUTES)).strftime('%Y-%m-%d %H:%M:%S')
        today_start = str(datetime.date.today()) + " 00:00:00"
    else:
        today_start = str(datetime.date.today()) + " 00:00:00" #如2025-12-19 00:00:00
    print("druid1 start_time: %s"%today_start)
    print("druid1 end time: %s"%cur_time)
    druid1_dict = {}
    for expinfo in exps:
        druid1_dict.setdefault(expinfo.druid1_id, []) #!!!,???,druid1_id是实时数据的id
        druid1_dict[expinfo.druid1_id].append(expinfo)
    exp_data = {} #存放实验数据:exp_data[exp_type + "_pst"][media_uuid + "_" + pst_uuid + "_" + lu_storeDsp_type] = [cost, income]
    total_cost = {} #total_cost[exp_type] = cost，每个实验总的消耗
    total_income = {} #total_income[exp_type] = income，每个实验总的收入
    for did, exps in druid1_dict.items():
        if exps[0].dim_info == "query" and query_list is not None:
            for i in range(0, len(query_list), 300):
                tmp_query_lsit = query_list[i: i+300]
                druid_param = get_druid1_param(did, today_start, cur_time, min_cost = min_cost,exps=exps,query_list=tmp_query_list)
                res_list = get_druid1_data(druid_param)
                if res_list is None:
                    continue
                for log_json in res_list:
                    lu_storeDsp_type = "1" if log_json['lu_storeDsp_type']=="1" else "0"
                    cost = log_json['lu_cost']
                    income = log_json['lu_income']
                    query = log_json['query']
                    exp_type = log_json["alpha_exp"]
                    if not exp_type:
                        continue
                    total_cost[exp_type] += cost
                    total_income[exp_type] += income
                    exp_data.setdefault(exp_type + "_query", {})
                    exp_data[exp_type + "_query"][query + "_" + lu_storeDsp_type] = [cost, income]
        elif exps[0].dim_info == "pst" and pst_id_list is not None:
            for i in range(0, len(pst_id_list), 300):
                tmp_pst_list = pst_id_list[i: i+300]
                druid_param = get_druid1_param(did, today_start, cur_time, min_cost = min_cost,exps=exps,pst_list=tmp_pst_list)
                res_list = get_druid1_data(druid_param)
                if res_list is None:
                    continue
                for log_json in res_list:
                    lu_storeDsp_type = "1" if log_json['lu_storeDsp_type']=="1" else "0" #!!!,???,lu_storeDsp_type表示二跳dsp广告位标识，，lu_storeDsp_type=1代表LU买量商店搜索三方dsp拆分独立广告位id
                    cost = log_json['lu_cost']
                    income = log_json['lu_income']
                    pst_uuid = log_json['pst_uuid']
                    media_uuid = log_json["media_uuid"]
                    exp_type = log_json["alpha_exp"]
                    if not exp_type:
                        continue
                    if exp_type == base_value:
                        exp_type = target_str
                    total_cost.setdefault(exp_type, 0)
                    total_cost[exp_type] += cost
                    total_income.setdefault(exp_type, 0)
                    total_income[exp_type] += income
                    exp_data.setdefault(exp_type + "_pst", {})
                    exp_data[exp_type + "_pst"][media_uuid + "_" + pst_uuid + "_" + lu_storeDsp_type] = [cost, income] #!!!,格式与线上服务要一致
        elif exps[0].dim_info == "pst1" and pst_id_list is not None:
            for i in range(0, len(pst_id_list), 300):
                tmp_pst_list = pst_id_list[i: i+300]
                druid_param = get_druid1_param(did, today_start, cur_time, min_cost = min_cost,exps=exps,pst_list=tmp_pst_list)
                res_list = get_druid1_data(druid_param)
                if res_list is None:
                    continue
                for log_json in res_list:
                    cost = log_json['lu_cost']
                    income = log_json['lu_income']
                    pst_uuid = log_json['pst_uuid']
                    exp_type = log_json["alpha_exp"]
                    if not exp_type:
                        continue
                    total_cost.setdefault(exp_type, 0)
                    total_cost[exp_type] += cost
                    total_income.setdefault(exp_type, 0)
                    total_income[exp_type] += income
                    exp_data.setdefault(exp_type + "_pst1", {})
                    exp_data[exp_type + "_pst1"][pst_uuid + "_0"] = [cost, income]
                    exp_data[exp_type + "_pst1"][pst_uuid + "_1"] = [cost, income]
        elif exps[0].dim_info == "flow":
            druid_param = get_druid1_param(did, today_start, cur_time, min_cost=min_cost, exps = exps)
            res_list = get_druid1_data(druid_param)
            for log_json in res_list:
                #lu_storeDsp_type = "1" if log_json['lu_storeDsp_type']=="1" else "0"
                cost = log_json['lu_cost']
                income = log_json['lu_income']
                dim_info = log_json['lu_media_type']
                #dim_info = log_json[exps[0].dim_info] if exps[0].dim_info in log_json else exps[0].dim_info #非大批量维度查询时，仅查询一次
                exp_type = log_json["alpha_exp"]
                if not exp_type:
                    continue
                total_income.setdefault(exp_type, 0)
                total_cost.setdefault(exp_type, 0)
                total_cost[exp_type] += cost
                total_income[exp_type] += income
                exp_data.setdefault(exp_type + "_flow", {})
                # 按dspType展开
                if 'lu_storeDsp_type' in log_json:
                    lu_storeDsp_type = "1" if log_json['lu_storeDsp_type']=="1" else "0"
                    exp_data[exp_type + "_flow"][dim_info + "_" + lu_storeDsp_type] = [cost, income]
                else:
                    exp_data[exp_type + "_flow"][dim_info + "_0"] = [cost, income]
                    exp_data[exp_type + "_flow"][dim_info + "_1"] = [cost, income]
        elif exps[0].dim_info == "flowPst": #!!!,flow和pst同时生效的实验,需要进一步开发
            dim_value_list = exps[0].dim_value #["1","2","11234"]
            tmp_pst_list = dim_value_list[2:]
            #首先获取media的消耗和收益数据
            media_or_pst_list = ["media", "pst"]
            for dim in media_or_pst_list:
                druid_param = get_druid1_param(did, today_start, cur_time, min_cost=min_cost, exps = exps, pst_list=tmp_pst_list, media_or_pst=dim)
                res_list = get_druid1_data(druid_param)
                if res_list is None:
                    continue
                for log_json in res_list:
                    #lu_storeDsp_type = "1" if log_json['lu_storeDsp_type']=="1" else "0"
                    cost = log_json['lu_cost']
                    income = log_json['lu_income']
                    dim_info = log_json['media_or_pst']
                    #dim_info = log_json[exps[0].dim_info] if exps[0].dim_info in log_json else exps[0].dim_info #非大批量维度查询时，仅查询一次
                    exp_type = log_json["alpha_exp"]
                    if not exp_type:
                        continue
                    if exp_type == base_value:
                        exp_type = target_str
                    total_income.setdefault(exp_type, 0)
                    total_cost.setdefault(exp_type, 0)
                    total_cost[exp_type] += cost
                    total_income[exp_type] += income
                    exp_data.setdefault(exp_type + "_flowPst", {})
                    # 按dspType展开
                    if 'lu_storeDsp_type' in log_json:
                        lu_storeDsp_type = "1" if log_json['lu_storeDsp_type']=="1" else "0" #!!!,???,lu_storeDsp_type表示二跳dsp广告位标识，，lu_storeDsp_type=1代表LU买量商店搜索三方dsp拆分独立广告位id
                        exp_data[exp_type + "_flowPst"][dim_info + "_" + lu_storeDsp_type] = [cost, income]
                    else:
                        exp_data[exp_type + "_flowPst"][dim_info + "_0"] = [cost, income] ##!!!,格式与线上服务要一致
                        exp_data[exp_type + "_flowPst"][dim_info + "_1"] = [cost, income]
        elif exps[0].dim_info == "compaigntype": #!!!,???,表示推广目标
            druid_param = get_druid1_param(did, today_start, cur_time, min_cost=min_cost, exps = exps)
            res_list = get_druid1_data(druid_param)
            for log_json in res_list:
                cost = log_json['lu_cost']
                income = log_json['lu_income']
                dim_info = log_json['lu_media_type']
                exp_type = log_json['alpha_exp']
                if not exp_type:
                    continue
                total_income.setdefault(exp_type, 0)
                total_cost.setdefault(exp_type, 0)
                total_cost[exp_type] += cost
                total_income[exp_type] += income
                exp_data.setdefault(exp_type + "_compaigntype", {})
                # compaintype 和 dsptype写死，后边改成配置
                exp_data[exp_type + "_compaigntype"][dim_info + "_1_0"] = [cost, income]
                exp_data[exp_type + "_compaigntype"][dim_info + "_1_1"] = [cost, income]
        elif exps[0].dim_info == "pkg":
            druid_param = get_druid1_param(did, today_start, cur_time, min_cost=min_cost, exps = exps)
            res_list = get_druid1_data(druid_param)
            if res_list is None:
                continue
            for log_json in res_list:
                cost = log_json['lu_iaa_cost']
                income = log_json['lu_iaa_income']
                dim_info = log_json['rpk_package']
                exp_type = log_json['alpha_exp']
                if not exp_type:
                    continue
                total_income.setdefault(exp_type, 0)
                total_cost.setdefault(exp_type, 0)
                total_cost[exp_type] += cost
                total_income[exp_type] += income
                exp_data.setdefault(exp_type + "_pkg", {})
                #实时数据media_type有问题,当前仅在联盟投放, 暂时hardcode 联盟
                exp_data[exp_type + "_pkg"]["2_" + dim_info] = [cost, income]

    for k in total_cost.keys():
        print("exp:", k, "total_cost:", total_cost[k], "total_income:", total_income[k])
    return exp_data

def get_pst_spent(pstIdLists, query_list, min_cost): #只用在process2函数
    cur_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())) + ":00"
    if datetime.datetime.now().hour == 0 and datetime.datetime.now().minute < COLD_START_MINUTES:
        #today_start = (datetime.datetime.now() - datetime.timedelta(days=0, minutes = COLD_START_MINUTES)).strftime('%Y-%m-%d %H:%M:%S')
        today_start = str(datetime.date.today()) + " 00:00:00"
    else:
        today_start = str(datetime.date.today()) + " 00:00:00"
    print("druid1 start_time: %s"%today_start)
    print("druid1 end time: %s"%cur_time)
    url = 'http://api.dataservice.prd.vivo.lan:8080/api/data/query'
    headers = {'Content-Type':'application/json'}
    res = dict({})
    total_cost = 0
    total_income = 0
    for i in range(0, len(pstIdLists), 400):
        tmp_pst_list = pstIdLists[i: i+400]
        data = {
        "appKey":"lu_real_effect_data",
        "appSecret":"8tkNQCiWUBi7H82G",
        "queryId":1308,
        "startTime":today_start,
        "endTime":cur_time,
        "pst_uuid": tmp_pst_list,
        "query_list": query_list,
        "min_cost": min_cost,
        }
        data_json = json.dumps(data)
        try:
            r = requests.post(url=url,headers=headers,data=data_json)
            r_new = json.loads(r.text)
            res_list = r_new['data']
        except:
            try_cnt = 0
            while try_cnt < 5:
                sleep(QPS_TIME)
                print("retry druid1: %d"%try_cnt)
                try:
                    r = requests.post(url=url,headers=headers,data=data_json)
                    print(r)
                    r_new = json.loads(r.text)
                    res_list = r_new['data']
                    break
                except:
                    try_cnt += 1
        if res_list is None:
            continue
        for log_json in res_list:
            cost = log_json['lu_cost']
            income = log_json['lu_income']
            pst_uuid = log_json['pst_uuid']
            media_uuid = log_json['media_uuid']
            total_cost += cost
            total_income += income
            res[media_uuid + "_" + pst_uuid] = [cost, income]
        sleep(QPS_TIME)
    print("total_cost:", total_cost)
    print("total_income:", total_income)
    return res

def get_browser_spent(queryLists, min_cost): #只用在process1函数
    cur_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())) + ":00"
    delta = datetime.timedelta(days=-1)
    #today_start = (datetime.datetime.strptime(cur_time, '%Y-%m-%d %H:%M:%S') + delta).strftime('%Y-%m-%d %H:%M:%S')
    #today_start = str(datetime.date.today()) + " 00:00:00"
    today_start = "2024-12-19 00:00:00"
    print("druid1 start_time: %s"%today_start)
    print("druid1 end time: %s"%cur_time)
    url = 'http://api.dataservice.prd.vivo.lan:8080/api/data/query'
    headers = {'Content-Type':'application/json'}
    res = dict({})
    total_cost = 0
    for i in range(0, len(queryLists), 1000):
        tmp_query_list = queryLists[i: i+1000]
        data = {
        "appKey":"lu_real_effect_data",
        "appSecret":"8tkNQCiWUBi7H82G",
        #"queryId":1304,
        "queryId":1584,
        "startTime":today_start,
        "endTime":cur_time,
        "queryList": tmp_query_list,
        "min_cost": min_cost,
        }
        #print(data)
        data_json = json.dumps(data)
        try:
            r = requests.post(url=url,headers=headers,data=data_json)
            r_new = json.loads(r.text)
            res_list = r_new['data']
        except:
            try_cnt = 0
            while try_cnt < 5:
                sleep(QPS_TIME)
                print("retry druid1: %d"%try_cnt)
                try:
                    r = requests.post(url=url,headers=headers,data=data_json)
                    print(r)
                    r_new = json.loads(r.text)
                    res_list = r_new['data']
                    break
                except:
                    try_cnt += 1
        for log_json in res_list:
            cost = log_json['lu_cost']
            clk_cnt = log_json['clk_cnt']
            query = log_json['query']
            total_cost += cost
            res[query] = [cost, clk_cnt]
        sleep(QPS_TIME)
    print("total_cost:", total_cost)
    return res

def get_curSpent_data(queryLists): #只用在process函数
    cur_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())) + ":00"
    #today_start = str(datetime.date.today()) + " 00:00:00"
    today_start = ""
    print("druid1 start_time: %s"%today_start)
    print("druid1 end time: %s"%cur_time)
    url = 'http://api.dataservice.prd.vivo.lan:8080/api/data/query'
    headers = {'Content-Type':'application/json'}
    res = dict({})
    total_cost = 0
    total_income = 0
    #count += 1
    for query in queryLists:
    # adx的数据
        data = {
        "appKey":"lu_real_effect_data",
        "appSecret":"8tkNQCiWUBi7H82G",
        "queryId":1293,
        "startTime":today_start,
        "endTime":cur_time,
        "queryList":[query],
        "min_income": DRUID_MIN_INCOME,
        "min_cost": DRUID_MIN_COST1,
        }
        data_json = json.dumps(data)
        try:
            r = requests.post(url=url,headers=headers,data=data_json)
            r_new = json.loads(r.text)
            res_list = r_new['data']
        except:
            try_cnt = 0
            while try_cnt < 5:
                sleep(QPS_TIME)
                print("retry druid1: %d"%try_cnt)
                try:
                    r = requests.post(url=url,headers=headers,data=data_json)
                    print(r)
                    r_new = json.loads(r.text)
                    res_list = r_new['data']
                    break
                except:
                    try_cnt += 1
                    continue
        # druid1限制每次查询结果为5000条，大于5000条会被截断,重新查询优先保证截断低消耗的部分
        if len(res_list) > 5000:
            print("query row cnt greater than 5000,  retry druid1.")
            sleep(QPS_TIME)
            data["min_cost"] = DRUID_MIN_COST2
            data_json = json.dumps(data)
            r = requests.post(url=url,headers=headers,data=data_json)
            r_new = json.loads(r.text)
            res_list = r_new['data']
        tmp_cost = 0
        tmp_income = 0
        for log_json in res_list:
            media_uuid = log_json["media_uuid"]
            pst_uuid = log_json["pst_uuid"]
            cost = log_json['lu_cost']
            media_type = log_json['lu_media_type']
            income = log_json['lu_income']
            total_cost += cost
            total_income += income
            tmp_cost += cost
            tmp_income += income
            res[query + "_" + media_type + "_" + media_uuid + "_" + pst_uuid] = [cost, income]
        sleep(QPS_TIME)
        print(query, len(res_list), tmp_cost, tmp_income)
    print("total_cost:", total_cost)
    print("total_income:", total_income)
    return res

def get_query_list(query_file): #从路径下面获取数据的set
    query_set = set([])
    with open(query_file, "r") as query_fp:
        for line in query_fp:
            query = line.strip()
            if query == '':
                continue
            query_set.add(query)
    return query_set

def process():
    query_set = get_query_list(sys.argv[1])
    real_data = get_curSpent_data(query_set)
    with open(sys.argv[2], "w") as fp:
        for k,v in real_data.items():
            fp.write("%s\t%.4f\t%.4f"%(k,v[0],v[1]))
            fp.write("\n")

def process1():
    query_set = get_query_list(sys.argv[1])
    real_data = get_browser_spent(list(query_set), 0)
    with open(sys.argv[2], "w") as fp:
        for k,v in real_data.items():
            fp.write("%s\t%.4f\t%d"%(k, v[0], v[1]))
            fp.write("\n")

def process2():
    pst_id_set = get_query_list(sys.argv[1])
    query_set = get_query_list(sys.argv[4])
    real_data = get_pst_spent(list(pst_id_set), list(query_set), 0)
    with open(sys.argv[2], "w") as fp:
        for k,v in real_data.items():
            fp.write("%s\t%.4f\t%.4f"%(k, v[0], v[1]))
            fp.write("\n")

def process3():
    pst_id_set = get_query_list(sys.argv[1])
    real_data = get_pst_landingpage_spent(list(pst_id_set), -1)
    with open(sys.argv[2], "w") as fp:
        for k, v in real_data.items():
            fp.write("%s\t%.4f\t%.4f"%(k, v[0], v[1]))
            fp.write("\n")

def process4():
    query_set = get_query_list(sys.argv[4])
    pst_set = get_query_list(sys.argv[1])
    real_exp_data = get_exp_pst_landingpage_spent(EXPINFO, query_list=list(query_set), pst_id_list=list(pst_set), min_cost=0)
    with open(sys.argv[2], "w") as fp:
        for exp_name,info in real_exp_data.items():
            for k,v in info.items():
                fp.write("%s\t%s\t%.4f\t%.4f"%(exp_name, k, v[0], v[1]))
                fp.write("\n")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage:python query_file out_res_file [mode]')
        exit(-1)
    if len(sys.argv) == 3:
        process()
    elif len(sys.argv) >= 4:
        if sys.argv[3] == "browser":
            process1()
        elif sys.argv[3] == "store_lu_pst":
            process2()
        elif sys.argv[3] == "store_lu_pst_landingpage":
            process3()
        elif sys.argv[3] == "real_ab_data":
            process4()
        else:
            pass
    else:
        pass
