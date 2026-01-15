'''
Description:
Author: zhaoshanshan
Date: 2023-08-17 19:04:39
'''
import os
import json
import sys
import time
from config import *


def get_druid_real_data(real_data_file):
    res = dict({})
    total_cost = 0
    total_income = 0
    with open(real_data_file, "r") as fp:
        for line in fp:
            row = line.split("\t")
            if len(row) != 3:
                continue
            cost = float(row[1])
            income = float(row[2])
            res[row[0]] = [cost, income]
            total_cost += cost
            total_income += income
    print("total_cost:", total_cost)
    print("total_income:", total_income)
    return res

def get_exp_druid1_real_data(real_data_file): #获取实验对应的德鲁伊实时数据（已经提前落在本地）
    res = dict({})
    with open(real_data_file, "r") as fp:
        for line in fp:
            row = line.strip().split("\t")
            res.setdefault(row[0],{})
            cost = float(row[2])
            income = float(row[3])
            res[row[0]][row[1]] = [cost, income] #!!!,PID_ALPHA_PST_AB_V1_pst 37d2556532f94a12975d6455c242ddaf_05270b01744d4df89f78e972036996d1_0     18.4832 115.4885
    return res

def get_store_druid_real_data(real_data_file):
    res = dict({})
    media_real_data = dict({})
    total_cost = 0
    total_income = 0
    with open(real_data_file, "r") as fp:
        for line in fp:
            row = line.split("\t")
            if len(row) != 3:
                continue
            dim_info = row[0].split("_")
            media_uuid = dim_info[0]
            pst_uuid = dim_info[1]
            cost = float(row[1])
            income = float(row[2])
            media_real_data.setdefault(media_uuid,[0,0])
            media_real_data[media_uuid][0] += cost
            media_real_data[media_uuid][1] += income
            res[row[0]] = [cost, income]
            total_cost += cost
            total_income += income
    print("total_cost:", total_cost)
    print("total_income:", total_income)
    return res, media_real_data, total_cost, total_income

def get_real_data(real_keys_set):
    real_profit_data = {}
    for key in real_keys_set:
        cost = redis_cli.hget(REAL_COST_DATA_REDIS_KEY_PREF, REAL_COST_DATA_FEAT_ID)
        income = redis_cli.hget(REAL_INCOME_DATA_REDIS_KEY_PREF, REAL_INCOME_DATA_FEAT_ID)
        real_profit_data[key] = [cost, income]
    return real_profit_data

def get_real_data(query_file, target_conf, day):
    query_scene_real_data = {}
    query_set = set()
    total_cost = 0
    total_income = 0
    with open(query_file, "r") as query_fp:
        for line in query_fp:
            query = line.strip()
            if query == '':
                continue
            query_set.add(query)
            for scene in target_conf.keys():
                #print(REAL_COST_DATA_REDIS_KEY_PREF  + query + "_" + scene + "_" + day)
                tmp_cost = redis_cli.hget(REAL_COST_DATA_REDIS_KEY_PREF + query + "_" + scene + "_" + day, REAL_COST_DATA_FEAT_ID)
                tmp_income = redis_cli.hget(REAL_INCOME_DATA_REDIS_KEY_PREF + query + "_" + scene + "_" + day, REAL_INCOME_DATA_FEAT_ID)
                query_scene_real_data.setdefault(query, {})
                if tmp_cost is not None:
                    cost = float(tmp_cost)*100
                else:
                    cost = 0.0
                if tmp_income is not None:
                    income = float(tmp_income)*100
                else:
                    income = 0.0
                total_cost += cost
                total_income += income
                query_scene_real_data[query][scene] = [cost, income]
                #print("%s\t%s\t%.4f\t%.4f"%(query, scene, cost, income))
        print("total_cost:", total_cost)
        print("total_income:", total_income)
    return query_scene_real_data, query_set

def get_query_list(query_file):
    query_set = set([])
    with open(query_file, "r") as query_fp:
        for line in query_fp:
            query = line.strip()
            if query == '':
                continue
            query_set.add(query)
    return query_set

def get_pst_params(pst_params_file):
    pst_params = {}
    with open(pst_params_file, "r") as fp:
        for line in fp:
            info = line.strip().split("\t")
            # p , i, d, upper, lower, target
            pst_params[info[0]] = [float(info[i]) for i in range(1, 7)] #!!!,001d0a4f174544fab166324565e52b81        0.6     0.015   0.02    0.5     6.5     1.15
    return pst_params

def get_pst_flow_map(pst_flow_file):
    pst_flow_map = {}
    with open(pst_flow_file, "r") as fp:
        for line in fp:
            info = line.strip().split("\t")
            pst_flow_map[info[0]] = [info[1]] #!!!,db804016bc63476e9e74a6b3f6119017        2
    return pst_flow_map

#短期固定参数，长期从文件加载
def get_media_param():
    media_uuid = {}
    pst_uuid = {}
    max_loss = 200000
    # p , i, d, upper, lower, target
    #media_uuid['4e4ce6c22443448ca00b9c01a379fc6b'] = [PID_P, PID_I, PID_D, 7.0, 0.5, 1.1]
    #pst_uuid['a38e77afa7324f008cbd60c28bbee6ab'] = [PID_P, PID_I, PID_D, 0.8, 8, 0.7, max_loss*0.8]
    #pst_uuid['9d09f7adaccb4513af93b5612fba7cd1'] = [PID_P, PID_I, PID_D, 0.5, 5.0, 0.8, max_loss*0.1]
    #pst_uuid['8b2a3affbcc049708c4c9676007ffbb0'] = [PID_P, PID_I, PID_D, 0.7, 5.0, 0.8, max_loss*0.1]
    #pst_uuid['64456d3db0c44da881e112e75987c82f'] = [PID_P*1.2, PID_I, PID_D, 0.5, 7.0, 0.8,  max_loss*0.3]
    #pst_uuid['3f68697b851d42d695e8c5fcb57f8bd3'] = [PID_P*1.5, PID_I, PID_D, 0.5, 7.0, 0.80, max_loss]
    #pst_uuid['5239602011434bae849e71c6ae5ffd19'] = [PID_P*1.2, PID_I, PID_D, 0.5, 7.0, 0.80, max_loss*0.1]
    return media_uuid, pst_uuid, max_loss
