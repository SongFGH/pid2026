'''
Description:
Author: zhaoshanshan
Date: 2023-04-17 19:04:39
'''
import os
import requests
import json
import sys
import time
import importlib
from config import *
from data_process import *
import datetime
import random
import threading
#importlib.reload(sys)

#max_threads = 10
#pool_sema = threading.BoundedSemaphore(max_threads)

#redis_client.sadd(name, value)：向指定的 Redis 集合键（key）中添加一个或多个成员（member）。
#Redis 客户端常见方法：
# redis_helper.hset,hset(name, key, value),用于向Redis哈希（Hash）结构中设置单个字段的值（也支持批量设置多个字段），哈希是键值对的嵌套结构（键→字段→值），如果哈希键不存在会先创建空哈希再设置字段
# redis_helper.sadd,sadd(name, value),用于向 Redis 集合（Set）结构中添加一个或多个元素,重复添加无效
# redis_helper.expire,expire(name, time),用于为任意类型的 Redis 键设置过期时间，如果name不存在则不会进行任何实质性操作（不会创建该name，也不会修改任何全局状态或其他name的属性）
# redis_helper.hget,hget(name, key),用于从 Redis 哈希结构中获取单个字段的值

def find_index(pst_value_list, pst_value, pst_flow_map):
    """
    查找目标值在列表中的索引，不存在则通过字典映射后二次查找
    :param pst_value_list: 待查找的原始列表
    :param pst_value: 初始要查找的目标值
    :param pst_flow_map: 映射字典，用于初始值不存在时获取替代值
    :return: 目标值/映射值在列表中的索引（int）；若均不存在，抛出ValueError异常
    """
    # 第一步：先判断初始pst_value是否在pst_value_list中
    if pst_value in pst_value_list:
        # 存在则直接返回其索引
        return pst_value_list.index(pst_value)
    
    # 第二步：初始值不存在，从pst_flow_map中获取对应的value
    # 先判断pst_value是否是字典的键（避免KeyError）
    if pst_value not in pst_flow_map:
        # 若初始值不在字典中，说明既无初始值也无映射值，抛出异常
        return -1
    
    # 获取映射后的目标值
    mapped_value = pst_flow_map[pst_value]
    
    # 第三步：判断映射后的value是否在pst_value_list中
    if mapped_value in pst_value_list:
        # 存在则返回映射值的索引
        return pst_value_list.index(mapped_value)
    else:
        return -1


def convert_to_float(input, default):
    if input is None or input == "":
        return default
    return float(input)

def get_target_ratio(): #!!!,???,从哪里获取数据,用在V0和V1
    target_conf = {}
    url = 'http://ads-strategy.vivo.lan:8080/strategy/config/v3'
    headers = {'Content-Type':'application/json'}
    data = {"appName":"LuProfitRatioAlgo",
        "dataVersionGt":-1}
    data_json = json.dumps(data,indent = 4,separators = (',',':'))
    r = requests.post(url=url,headers=headers,data=data_json)
    r_new = json.loads(r.text)
    config = r_new["configList"][0]
    strategy_config=json.loads(config['strategyConfig'])
    for value in strategy_config:
        version = value["version"]
        target_ratio = float(value["target_ratio"])
        share_ratio = float(value["share_ratio"])
        scene = str(value["scene"])
        target_conf.setdefault(scene, [])
        target_conf[scene].append([version, target_ratio, share_ratio]) #!!!,{'1': [['feedsV0', 1.1, 0.93], ['feedsV1', 1.1, 0.93]], '2': [['unionV0', 1.1, 0.93], ['unionV1', 1.1, 0.93]]}
    print(target_conf)
    return target_conf

def get_preday_key(pid_redis_key): #传入的Redis键名字符串，函数的返回值是前一天的Redis键名  
    key_parts = pid_redis_key.split("_")
    pre_day = datetime.datetime.strptime(key_parts[-1], '%Y%m%d') + datetime.timedelta(days=-1) #计算前一天的日期
    pre_day_str = pre_day.strftime('%Y%m%d') #将前一天的日期转换为字符串
    pid_redis_key_new = "_".join(key_parts[:-1] + [pre_day_str]) #重新拼接新的 Redis 键名
    return pid_redis_key_new

def get_cold_start_param(pid_redis_key, pid_redis_field_pre, pid_redis_key_assist, redis_helper):
    pid_redis_key_new = get_preday_key(pid_redis_key) #存放control参数的key，exp_name + "_" + day,
    pid_redis_key_assist_new = get_preday_key(pid_redis_key_assist) #存放pid参数的key，exp_name + "_assist_" + day,
    control = convert_to_float(redis_helper.hget(pid_redis_key_new, pid_redis_field_pre + '_control'), 1.0) #hget(key, field)：Redis哈希操作，返回键key中字段field对应的value（不存在则返回None）
    err_last = convert_to_float(redis_helper.hget(pid_redis_key_assist_new, pid_redis_field_pre + '_err'), 0.0)
    #err_last = convert_to_float(redis_helper.hget(pid_redis_key_new, pid_redis_field_pre + '_err'), 0.0)
    err_sum = convert_to_float(redis_helper.hget(pid_redis_key_assist_new, pid_redis_field_pre + '_err_sum'), 0.0)
    #err_sum = convert_to_float(redis_helper.hget(pid_redis_key_new, pid_redis_field_pre + '_err_sum'), 0.0)
    print("old control", control)
    # noted on 20230913
    #control = max(0.1, control)
    #if control > 1.2:
    #    if err_last > 0:
    #        control = 1.2
    #    else:
    #        control = max(1.2, control-1)
    #elif control < 0.1:
    #    if err_last > 0:
    #        control = 0.05
    #    else:
    #        control = 0.1
    return control, err_last, err_sum

def circuit_breaker_param(pid_redis_key, pid_redis_field_pre, pid_redis_key_assist, redis_helper):
    #redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_err", 0)
    redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err", 0)
    #redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_err_sum", 0)
    redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err_sum", 0)
    #redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_err_delta", 0)
    redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err_delta", 0)
    redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_control", BREAK_TLOWER) #BREAK_TLOWER = 0.07

def pid_control(real_cost, real_income, target, real_ratio, pid_redis_key, pid_redis_field_pre, pid_redis_key_assist ,redis_helper, tupper, tlower, pp=PID_P, pi=PID_I, pd=PID_D):
    #pid_redis_key = exp_name + "_" + day, 
    #pid_redis_field_pre = k + "_all_query", 
    #pid_redis_key_assist = exp_name + "_assist_" + day,
    #pool_sema.acquire()
    out_log = []
    cold_start = (datetime.datetime.now().hour == 0 and datetime.datetime.now().minute < COLD_START_MINUTES) #判断是否处于冷启动时段
    #逻辑可废弃,如果key里无day不能废弃
    if real_cost < SMOOTH_N/2 and cold_start:
        print("cold start...")
        cold_start_res = get_cold_start_param(pid_redis_key, pid_redis_field_pre, pid_redis_key_assist, redis_helper)
        #redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_err", 0)
        redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err", 0) #
        #redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_err_sum", 0)
        redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err_sum", 0) #
        #redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_err_delta", 0)
        redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err_delta", 0) #
        redis_helper.hset(pid_redis_key, pid_redis_field_pre + "_control", cold_start_res[0])
        out_log = [0, 0, 0.5, 0, 0, pid_redis_key, pid_redis_field_pre,  cold_start_res[0], 0, 0, 0, 0]
        print(" ".join(["%s"%str(i) for i in out_log]))
        return
    control = convert_to_float(redis_helper.hget(pid_redis_key, pid_redis_field_pre + '_control'), -2)
    if control == -2: #如果
        print("pst id cold start...")
        cold_start_res = get_cold_start_param(pid_redis_key, pid_redis_field_pre, pid_redis_key_assist, redis_helper)
        control = cold_start_res[0]
    #err_last = convert_to_float(redis_helper.hget(pid_redis_key, pid_redis_field_pre + '_err'), 0.0)
    err_last = convert_to_float(redis_helper.hget(pid_redis_key_assist, pid_redis_field_pre + '_err'), 0.0) #hget(key, field)：Redis哈希操作，返回键key中字段field对应的value（不存在则返回None）
    #err_sum = convert_to_float(redis_helper.hget(pid_redis_key, pid_redis_field_pre + '_err_sum'), 0.0)
    err_sum = convert_to_float(redis_helper.hget(pid_redis_key_assist, pid_redis_field_pre + '_err_sum'), 0.0)
    out_log = [real_cost, real_income, control, err_last, err_sum, pid_redis_key, pid_redis_field_pre]
    #print('before pid:', control, err_last, err_sum, pid_redis_key, pid_redis_field_pre)
    err = 1 - (real_ratio / target)
    #err = target - real_all
    #xx = datetime.datetime.now().hour / (24.0 - datetime.datetime.now().hour)
    #err = 1 - real_incre / max((target + xx*(target - real_all)),0.01)
    #err = ((target + xx*(target - real_all)) - real_incre) / max(abs(target + xx*(target - real_all)),0.1)
    if err * err_sum < 0:
    #if err * err_last < 0:
        err_sum = 0 * err_sum
    else:
        err_sum = err_sum + err
    err_delta = err - err_last
    #err = max(min(0.1, err),-0.1)
    err_sum = max(min(10, err_sum),-10)
    pid_out = pp * err + pi * err_sum + pd * err_delta
    pid_out = pid_out * 1.0
    #真实消费达到一定数值，利润额正向后,保证小步往上调,避免调整幅度太大,利润额抖降
    #if real_cost > MAX_POS_COST and control > MIN_CTRL and pid_out <= PID_OUT_THRESH:
    #    print("control adjust step...")
    #    #pid_out = -0.15
    #    pid_out = PID_OUT_THRESH
    control = control * (1 - pid_out)
    #control =  1.0 / (1.0/control - pid_out)
    if control > tupper:
        control = tupper
    if control < tlower:
        control = tlower
        explore_rate = random.randint(1,20)/20
        print("explore_rate:",explore_rate)
        if real_cost > MIN_EXPLORE_COST and explore_rate <= 0.1:
            print("explore..")
            control = random.randint(1,2)/10
    # todo: 考虑如何避免某一维度被熔断后无法出量的问题
    if real_cost > MAX_BREAK_COST and real_ratio < MIN_BREAK_PROFIT_RATIO and real_ratio < target and control > BREAK_TLOWER: #BREAK_TLOWER = 0.07
        print("enter circuit breaker...")
        circuit_breaker_param(pid_redis_key, pid_redis_field_pre, pid_redis_key_assist, redis_helper)
        out_log += [BREAK_TLOWER, 0.0, 0.0, 0.0, 0.0] #BREAK_TLOWER = 0.07
        print(" ".join(["%s"%str(i) for i in out_log]))
        return
    #err_sum = 0
    #pid_redis_key = exp_name + "_" + day, 
    #pid_redis_field_pre = k + "_all_query", 
    #pid_redis_key_assist = exp_name + "_assist_" + day,
    redis_helper.hset(pid_redis_key, pid_redis_field_pre + '_control', control) #hset(key, field, value)：Redis哈希操作，将键key中的字段field设置为value
    redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + '_err', err)
    redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err_sum", err_sum)
    redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_err_delta", err_delta)
    redis_helper.hset(pid_redis_key_assist, pid_redis_field_pre + "_pid_out", pid_out)
    out_log += [control, err, err_sum, err_delta, pid_out]
    print(" ".join(["%s"%str(i) for i in out_log]))
    return control
    #pool_sema.release()  # 解锁

def process():
    #python ../pysrc/pid_control_v6.py ${day} ${QUERYDIR}/${lu_query_file_path} V5 ${REALDATADIR}/${lu_real_data_file_path} ${PSTCONF}/${lu_pst_pid_info_path} >  logs/V6_${day}_${minutes}.log
    day = sys.argv[1]
    query_file = sys.argv[2]
    version = sys.argv[3]
    target_conf = get_target_ratio() #!!!,用在V0和V1
    if version == "V0":
        query_scene_real_data, query_set = get_real_data(query_file, target_conf, day)
        for k in query_scene_real_data.keys():
            for s,v in query_scene_real_data[k].items():
                for item in target_conf[s]:
                    if item[0].find(version) == -1:
                        continue
                    target_ratio = item[1]
                    version_name = item[0]
                    share_ratio = item[2]
                    smooth_profit_ratio2 = (v[1]*share_ratio + max(SMOOTH_N - v[0], 0) * target_ratio) / (v[0] + max(SMOOTH_N - v[0], 0))
                    print("real data:", v[0], v[1])
                    pid_control(v[0], v[1], target_ratio, smooth_profit_ratio2, PID_REDIS_KEY_PREF + version_name + "_" + day, k + "_" + s, PID_REDIS_KEY_ASSIST_PREF + version_name + "_" + day, redis_cli_pre)
    elif version == "V1":
        real_data_file = sys.argv[4]
        start_time = time.time()
        real_data = get_druid_real_data(real_data_file)
        print("load data total cost:", time.time()-start_time)
        threads_list = []
        num = 0
        for k,v in real_data.items():
            k_item = k.split("_")
            for item in target_conf[k_item[1]]:
                if item[0].find(version) == -1:
                    continue
                target_ratio = item[1]
                share_ratio = item[2]
                version_name = item[0]
                smooth_profit_ratio2 = (v[1]*share_ratio + max(SMOOTH_N - v[0], 0) * target_ratio) / (v[0] + max(SMOOTH_N - v[0], 0))
                pid_control(v[0], v[1], target_ratio, smooth_profit_ratio2, PID_REDIS_KEY_PREF + version_name + "_" + day, k_item[0] + "_" + k_item[2] + "_" + k_item[3], PID_REDIS_KEY_ASSIST_PREF + version_name + "_" + day, redis_cli, TUPPER, TLOWER)
                #t = threading.Thread(target=pid_control, args=(v[0], v[1], target_ratio, smooth_profit_ratio2, PID_REDIS_KEY_PREF + version_name + "_" + day, k_item[0] + "_" + k_item[2] + "_" + k_item[3], PID_REDIS_KEY_ASSIST_PREF + version_name + "_" + day, redis_cli))
                #threads_list.append(t)
                num += 1
        #for t in threads_list:
            #t.setDaemon(True)
            #t.start()
        #for t in threads_list:
            #t.join()
    elif version == "V2":
        real_data_file = sys.argv[4]
        real_data = get_druid_real_data(real_data_file)
        browser_target_ratio = 1.25
        #redis_helper = redis_cli_pre
        redis_helper = redis_cli
        cost = 0
        income = 0
        for k,v in real_data.items():
            cost += v[0]
            # 0.038为50*0.96*0.8/1000，100为将元转为分
            income += v[1] * 0.038 * 100
        #浏览器整体调节利润率
        print("real data:", cost, income)
        smooth_profit_ratio2 = (income + max(BROWSER_SMOOTH_N - cost, 0) * browser_target_ratio) / (cost + max(BROWSER_SMOOTH_N - cost, 0))
        control = convert_to_float(redis_helper.hget("browser_v0_" + day,  'all_query_control'), -1)
        print("control", control, "smooth_profit_ratio2", smooth_profit_ratio2)
        if control > 0:
            pid_control(cost, income, browser_target_ratio, smooth_profit_ratio2, "browser_v0_" + day, "all_query", "browser_v00_" + day, redis_helper, BROWSER_TUPPER, BROWSER_TLOWER, pp=BROWSER_PID_P)
            redis_helper.expire("browser_v00_" + day, 3600*24*2)
            control = convert_to_float(redis_helper.hget("browser_v0_" + day,  'all_query_control'), -1)
        else:
            control = 1.0
            redis_helper.hset("browser_v0_" + day, "all_query_control", 1.0)
        print("control", control)
        #control = convert_to_float(redis_helper.hget("browser_v0_" + day,  'all_query_control'), 1.0)
        for k,v in real_data.items():
            cost = v[0]
            income = v[1] * 0.038 * 100
            #smooth_profit_ratio2 = (income + max(SMOOTH_N - v[0], 0) * target_ratio) / (v[0] + max(SMOOTH_N - v[0], 0))
            redis_helper.hset("browser_v0_" + day, k + "_control", control)
            redis_helper.expire("browser_v0_" + day, 3600*24*2)
            out_log = [cost, income, control, 0, 0, "browser_v0_" + day, k,  control, 0, 0, 0, 0]
            print(" ".join(["%s"%str(i) for i in out_log]))
            #pid_control(cost, income, target_ratio, smooth_profit_ratio2, "browser_v0_" + day, k, "browser_v00_" + day, redis_cli_pre)
    elif version == "V3":
        redis_helper = redis_cli_pre
        #redis_helper = redis_cli
        real_data_file = sys.argv[4]
        real_data = get_druid_real_data(real_data_file)
        query_set = get_query_list(query_file)
        pst_params = get_pst_params(sys.argv[5])
        target_ratio = 1.2
        cost = 0
        income = 0
        pp = PID_P
        pi = PID_I
        pd = PID_D
        lower = TLOWER
        upper = TUPPER
        for k,v in real_data.items():
            pst_id = k.split("_")[1]
            if pst_id in pst_params:
                pp = pst_params[pst_id][0]
                pi = pst_params[pst_id][1]
                pd = pst_params[pst_id][2]
                lower = pst_params[pst_id][3]
                upper = pst_params[pst_id][4]
                target = pst_params[pst_id][5]
            cost = v[0]
            # 0.038为50*0.96*0.8/1000，100为将元转为分
            income = v[1]
            smooth_profit_ratio2 = (income + max(SMOOTH_N - cost, 0) * target_ratio) / (cost + max(SMOOTH_N - cost, 0))
            pid_control(cost, income, target_ratio, smooth_profit_ratio2, "store_lu_pst_v0_" + day, k + "_all_query", "store_lu_pst_v0_assist_" + day, redis_helper, upper, lower, pp=pp, pi=pi, pd=pd)
            redis_helper.expire("store_lu_pst_v0_" + day, 3600*24*2)
            redis_helper.expire("store_lu_pst_v0_assist_" + day, 3600*24*2)
            control = convert_to_float(redis_helper.hget("store_lu_pst_v0_" + day,  k + '_all_query_control'), -1)
            dict_value = {}
            for q in query_set:
                dict_value[q + "_" + k + "_control"] = control
                out_log = [cost, income, control, 0, 0, "store_lu_pst_q_v0_" + day, q + "_" + k,  control, 0, 0, 0, 0]
                print(" ".join(["%s"%str(i) for i in out_log]))
            #redis_helper.hmset("store_lu_pst_v0_" + day, dict_value)
            redis_helper.hset("store_lu_pst_q_v0_" + day, mapping=dict_value)
            redis_helper.expire("store_lu_pst_q_v0_" + day, 3600*24*2)
    elif version == "V5":
        redis_helper = redis_cli
        real_data_file = sys.argv[4]
        #获取实验对应的德鲁伊实时数据（已经提前落在本地）,ll /data7/11147482/lu_ad_stategy/lu_profit_ratio_opt/data/real_data/store_ad_pst/real_ab_data_*
        real_data = get_exp_druid1_real_data(real_data_file) #real_data是词典
        query_set = get_query_list(query_file) #获取query构成的set，似乎没有使用到
        pst_params = get_pst_params(sys.argv[5]) #!!!,???,获取pid的调节参数，提前设置好的,pst_params是词典
        media_uuids, pst_uuids, max_loss = get_media_param() #!!!,???,固定参数,似乎没有使用到
        pst_flow_file = sys.argv[6]
        pst_flow_map = get_pst_flow_map(pst_flow_file)
        pp = PID_P #0.08
        pi = PID_I #0.015
        pd = PID_D #0.02
        lower = TLOWER #0.001
        upper = TUPPER #4.7
        m_control = {}
        total_cost = 0 #存放所有实验组总的消耗
        total_income = 0 #存放所有实验组总的收入
        for exp in EXPINFO:
            exp_name = exp.exp_name
            dim_value_list = exp.dim_value #["1","2","11"]
            target_ratio_list = exp.target_ratio #[1.15,1.16,1.17]
            lower_list = exp.lower #[0.7,0.7,0.7]
            upper_list = exp.upper #[1.2,1.2,1.2]
            print("exp name:%s\t%s"%(exp_name,exp.dim_info))
            element_str = ", ".join(exp.dim_value)
            print("exp.dim_value:", element_str)
            #执行SADD命令：向名为“exp_name_kset_day”的集合中添加元素“exp_name_day”
            #exp_name + "_kset_" + day，这种写法需要和服务配置的lu_pid_parms.conf一致
            redis_helper.sadd(exp_name + "_kset_" + day, exp_name + "_" + day)
            redis_helper.expire(exp_name + "_kset_" + day, 3600*24*2) #执行EXPIRE命令：为上述集合键设置过期时间，时间为2天（3600秒*24小时*2天）
            tmp_cost = 0 #存放特定实验组总的消耗
            tmp_income = 0 #存放特定实验组总的收入
            data_key = exp_name + "_" + exp.dim_info
            #real_data[exp_type + "_pst"][media_uuid + "_" + pst_uuid + "_" + lu_storeDsp_type] = [cost, income],广告位
            #exp_data[exp_type + "_flow"][dim_info + "_" + lu_storeDsp_type] = [cost, income],媒体类型
            #exp_data[exp_type + "_flowPst"]
            if data_key not in real_data: 
                continue
            for k,v in real_data[data_key].items():
                #k=media_uuid + "_" + pst_uuid + "_" + lu_storeDsp_type,广告位
                #k=dim_info + "_" + lu_storeDsp_type,媒体类型
                dim_v = k.split("_")
                if len(dim_v) <= 1: #跳过异常值，比如媒体类型为空
                    print("warning!!! skill k=", k, "v=", v)
                    continue; 
                ludsp_type = dim_v[-1] #lu_storeDsp_type表示二跳dsp广告位标识，，lu_storeDsp_type=1代表LU买量商店搜索三方dsp拆分独立广告位id
                dim = dim_v[-2]
                if exp.dim_info == "pst":
                    media_id = dim_v[-3]
                    pst_id = dim
                pst_index = find_index(dim_value_list, dim, pst_flow_map)
                target_ratio = target_ratio_list[pst_index]
                lower = lower_list[pst_index]
                upper = upper_list[pst_index]
                # 主启用个性化，新落地页用全局的pid参数配置
                if ludsp_type == "0" and exp.dim_info == "pst" and pst_id in pst_params: #!!!,???,主启用个性化，新落地页用全局的pid参数配置，
                #if exp.dim_info == "pst" and pst_id in pst_params:
                    pp = pst_params[pst_id][0]
                    pi = pst_params[pst_id][1]
                    pd = pst_params[pst_id][2]
                    #lower = pst_params[pst_id][3]
                    #upper = pst_params[pst_id][4]
                    #target_ratio = pst_params[pst_id][5]
                cost = v[0]
                # 0.038为50*0.96*0.8/1000，100为将元转为分
                income = v[1]
                total_cost += cost
                total_income += income
                tmp_cost += cost
                tmp_income += income
                print("adjust pst profit target:", target_ratio, upper, lower)
                smooth_profit_ratio2 = (income + max(SMOOTH_N - cost, 0) * target_ratio) / (cost + max(SMOOTH_N - cost, 0)) #!!!,???,这行代码啥作用
                if exp.dim_info == "pkg":
                    print("dxx:pkg:",cost, income, target_ratio, smooth_profit_ratio2, exp_name + "_" + day, k + "_all_query",exp_name + "_assist_" + day, redis_helper, upper, lower)
                control = pid_control(cost, income, target_ratio, smooth_profit_ratio2, exp_name + "_" + day, k + "_all_query", exp_name + "_assist_" + day, redis_helper, upper, lower, pp=pp, pi=pi, pd=pd)
                #control = convert_to_float(redis_helper.hget(exp_name + "_" + day,  k + '_all_query_control'), 1.0)
                cur_time = time.strftime('%Y%m%d%H%M', time.localtime(time.time())) + "00" #最终格式为"年月日时分秒"（如20251219153000）
                #将前一天的参数平移到第二天
                if cur_time >= day+"235500" and cost > 5000: #满足当前时间达到某日的 23:55且成本超过 5000这两个条件
                    delta = datetime.timedelta(days=1) #创建一个表示1天时间间隔的时间增量对象
                    next_day = (datetime.datetime.strptime(day, '%Y%m%d') + delta).strftime('%Y%m%d') #得到当前day的下一天日期字符串
                    redis_helper.hset(exp_name + "_" + next_day,  k + '_all_query_control', control) #将控制参数提前存入下一天的 Redis 哈希表中，供下一天的业务逻辑读取使用
                    print("put control to nextday:" + next_day)
            print("exp cost income:", tmp_cost, tmp_income) #打印改实验的收入和消耗
            if  cur_time >= day+"235500":#当前时间是否达到某日 23:55
                redis_helper.sadd(exp_name + "_kset_" + next_day, exp_name + "_" + next_day) #提前将下一天的业务标识存入对应的Redis集合中，
            redis_helper.expire(exp_name +  "_assist_" + day, 3600*24*2) #为当日的Redis键设置2天过期时间，key不存在的话不进行任何操作
            redis_helper.expire(exp_name + "_" + day, 3600*24*2) #为当日的Redis键设置2天过期时间，key不存在的话不进行任何操作
        print("total cost income:", total_cost, total_income)

    elif version == "V4":
        #redis_helper = redis_cli_pre
        redis_helper = redis_cli
        real_data_file = sys.argv[4]
        real_data, media_real_data, total_cost, total_income = get_store_druid_real_data(real_data_file)
        query_set = get_query_list(query_file)
        pst_params = get_pst_params(sys.argv[5])
        media_uuids, pst_uuids, max_loss = get_media_param()
        from chinese_calendar import is_holiday, is_workday
        cur_day = datetime.datetime.strptime(day,'%Y%m%d')
        if day < '20240101':
            global_target_ratio = 1.1 if is_workday(cur_day) else 1.1
        else:
            global_target_ratio = 1.1
        cost = 0
        income = 0
        pp = PID_P
        pi = PID_I
        pd = PID_D
        lower = TLOWER
        upper = TUPPER
        a=dict()
        m_control = dict()
        media_key = "store_lu_media_v0_"
        for m,v in media_uuids.items():
            cost_income = media_real_data.get(m, [0, 0])
            cost = cost_income[0]
            income = cost_income[1]
            smooth_profit_ratio2 = (income + max(total_income*0.03 - income, 0)) / (cost + max(total_cost*0.03 - cost, 0))
            #smooth_profit_ratio2 = (income + max(SMOOTH_N - cost, 0) * v[5]) / (cost + max(SMOOTH_N - cost, 0))
            print(smooth_profit_ratio2)
            pid_control(cost, income, v[5], smooth_profit_ratio2, media_key + day, m + "_m", media_key + "assist_" + day, redis_helper, v[3], v[4], pp=v[0], pi=v[1], pd=v[2])
            redis_helper.expire(media_key + day, 3600*24*2)
            redis_helper.expire(media_key + "assist_" + day, 3600*24*2)
            control = convert_to_float(redis_helper.hget(media_key + day,  m + '_m_control'), 1.0)
            m_control[m] = control if (cost - income) < max_loss else 0.1
            redis_helper.hset(media_key + day,  m + '_m_control', m_control[m])
        for k,v in real_data.items():
            media_id = k.split("_")[0]
            pst_id = k.split("_")[1]
            ludsp_type = k.split("_")[2]
            exp_type = k.split("_")[3]
            target_ratio = global_target_ratio
            lower = TLOWER
            upper = TUPPER
            # 主启用个性化，新落地页用全局的pid参数配置
            if ludsp_type == "0" and pst_id in pst_params:
                pp = pst_params[pst_id][0]
                pi = pst_params[pst_id][1]
                pd = pst_params[pst_id][2]
                lower = pst_params[pst_id][3]
                upper = pst_params[pst_id][4]
                target_ratio = pst_params[pst_id][5]
            cost = v[0]
            # 0.038为50*0.96*0.8/1000，100为将元转为分
            income = v[1]
            # todo: pstid消耗过大的,单独调节
            if ludsp_type == "0" and media_id in m_control and pst_id not in pst_uuids:
                control = m_control[media_id]
                redis_helper.hset("store_lu_pst_v0_" + day, k + "_all_query_control", control)
                out_log = [cost, income, control, 0, 0, "store_lu_pst_v0_" + day, k + "_all_query",  control, 0, 0, 0, 0]
                print(" ".join(["%s"%str(i) for i in out_log]))
            else:
                # 七猫优质广告位前期单独设置低利润率，进行补贴，且补贴额度小于阈值，大于阈值后,用大盘利润率目标，大概率会熔断
                if ludsp_type == "0" and pst_id in pst_uuids and pst_uuids[pst_id][6] > (cost - income):
                    upper = pst_uuids[pst_id][4]
                    lower = pst_uuids[pst_id][3]
                    target_ratio = pst_uuids[pst_id][5]
                    print("adjust pst profit target:", target_ratio, upper, lower)
                print("adjust pst profit target:", target_ratio, upper, lower)
                if exp_type in EXPINFO:
                    target_ratio = EXPINFO[exp_type]
                smooth_profit_ratio2 = (income + max(SMOOTH_N - cost, 0) * target_ratio) / (cost + max(SMOOTH_N - cost, 0))
                pid_control(cost, income, target_ratio, smooth_profit_ratio2, "store_lu_pst_v0_" + day, k + "_all_query", "store_lu_pst_v0_assist_" + day, redis_helper, upper, lower, pp=pp, pi=pi, pd=pd)
                redis_helper.expire("store_lu_pst_v0_" + day, 3600*24*2)
                redis_helper.expire("store_lu_pst_v0_assist_" + day, 3600*24*2)
                control = convert_to_float(redis_helper.hget("store_lu_pst_v0_" + day,  k + '_all_query_control'), 1.0)
            cur_time = time.strftime('%Y%m%d%H%M', time.localtime(time.time())) + "00"
            # support A/B
            #redis_helper.srem("lu_pst_set_" + day, "store_lu_pst_v0_" + day)
            redis_helper.sadd("lu_pst_set_" + day, "store_lu_pst_v0_" + day)
            redis_helper.expire("lu_pst_set_" + day, 3600*24*2)
            #将前一天的参数平移到第二天
            if cur_time >= day+"235500" and cost > 5000:
                delta = datetime.timedelta(days=1)
                next_day = (datetime.datetime.strptime(day, '%Y%m%d') + delta).strftime('%Y%m%d')
                print("put control to nextday:" + next_day)
                redis_helper.hset("store_lu_pst_v0_" + next_day,  k + '_all_query_control', control)
                #redis_helper.srem("lu_pst_set_" + day, "store_lu_pst_v0_" + day)
                redis_helper.sadd("lu_pst_set_" + next_day, "store_lu_pst_v0_" + next_day)
                redis_helper.expire("lu_pst_set_" + next_day, 3600*24*2)
            '''dict_value = {}
            dict_value[ k + "_all_query_control"] = control
            for q in query_set:
                dict_value[q + "_" + k + "_control"] = control
                out_log = [cost, income, control, 0, 0, "store_lu_pst_q_v0_" + day, q + "_" + k,  control, 0, 0, 0, 0]
                print(" ".join(["%s"%str(i) for i in out_log]))
            #redis_helper.hmset("store_lu_pst_v0_" + day, dict_value)
            redis_helper.hset("store_lu_pst_q_v0_" + day, mapping=dict_value)
            redis_helper.expire("store_lu_pst_q_v0_" + day, 3600*24*2)'''
    else:
        pass
if __name__=='__main__':
    if len(sys.argv) < 4:
        print('Usage:python pid day query_file version [data_file]')
        sys.exit()
    process()

