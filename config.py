'''
Description:
Author: zhaoshanshan
Date: 2023-04-17 19:04:39
'''
import os
import datetime
import json
import requests
import sys
import time
from rediscluster import RedisCluster
from exp_version import ExpInfo

# redis key 数据配置
PID_REDIS_KEY_PREF = "lu_profit_alpha_" #!!!,
PID_REDIS_KEY_ASSIST_PREF = "lu_profit_alpha1_" #!!!,
REAL_COST_DATA_REDIS_KEY_PREF = "lu_cost_" #!!!,
REAL_COST_DATA_FEAT_ID = 5302 #!!!,
REAL_INCOME_DATA_REDIS_KEY_PREF = "lu_income_" #!!!,
REAL_INCOME_DATA_FEAT_ID = 5303 #!!!,
REDIS_NODES_PRODUCT=[ #!!!,
    {"host":"ads-lu-feats2-prd-prd0.china.redis.dba.vivo.lan.","port":11123},
    {"host":"ads-lu-feats2-prd-prd0.china.redis.dba.vivo.lan.","port":11131},
    {"host":"ads-lu-feats2-prd-prd0.china.redis.dba.vivo.lan.","port":11121}
]

REDIS_NODES_TEST = [{"host": "cpd-predictor-userinfo-pre-pre0.redis.dba.vivo.lan.","port":11378}, #!!!,
        {"host": "cpd-predictor-userinfo-pre-pre1.redis.dba.vivo.lan.","port":11379},
        {"host": "cpd-predictor-userinfo-pre-pre2.redis.dba.vivo.lan.", "port": 11380}
        ]
REDIS_NODES_TEST1 = [{"host": "ads-common-feat-pre-pre0.redis.dba.vivo.lan.","port":11195}, #!!!,
        {"host": "ads-common-feat-pre-pre1.redis.dba.vivo.lan.","port":11235},
        {"host": "ads-common-feat-pre-pre3.redis.dba.vivo.lan.", "port":11165 },
        {"host": "ads-common-feat-pre-pre4.redis.dba.vivo.lan.", "port":11110 },
        {"host": "ads-common-feat-pre-pre2.redis.dba.vivo.lan.", "port":11206 }
        ]

# 初始化Redis集群客户端，连接生产环境的Redis集群节点
# startup_nodes：指定Redis集群的启动节点列表（配置在REDIS_NODES_PRODUCT中）
# decode_responses=True：让Redis返回的结果自动从字节串解码为字符串
redis_cli = RedisCluster(startup_nodes=REDIS_NODES_PRODUCT, decode_responses=True)
#redis_cli_pre = RedisCluster(startup_nodes=REDIS_NODES_TEST1, decode_responses=True)


# pid调节策略超参
PID_P = 0.08
#PID_I = 0.01
#PID_D = 0.008
PID_I = 0.015
PID_D = 0.02
SMOOTH_N = 30000
COLD_START_MINUTES = 30
TUPPER = 4.7
TLOWER = 0.001
#TLOWER = 0.05
#熔断策略
#MAX_BREAK_COST = 80000
MAX_BREAK_COST = 3000000
MIN_BREAK_PROFIT_RATIO = 0.60
BREAK_TLOWER = 0.07
#随机探索
MIN_EXPLORE_COST = 3000000
#控制正向调节幅度
MAX_POS_COST = 50000
MIN_CTRL = 2.0
PID_OUT_THRESH = -0.05

#druid1取数控制,单位分
DRUID_MIN_INCOME = -1
DRUID_MIN_COST1 = 0
DRUID_MIN_COST2 = 100
QPS_TIME = 0.09

BROWSER_PID_P=1.8
BROWSER_SMOOTH_N = 50000
BROWSER_TUPPER = 4.0
BROWSER_TLOWER = 0.7

# expName和线上流量控制一致，考虑同一实验名称下，走不同调控策略的实时数据流情况。不同维度对应druid1id不能相同
# expID, onlineExpName, dim, dim_value, target_ratio, druid1_id #!!!,???,每一个实验代表什么意思，和实验管理平台怎么对应，如何设置
EXPINFO = [
        #     ExpInfo(1, "PID_ALPHA_PST_AB_V1", "pst", 1.15, 1368, lower=0.7, upper=1.2), # 1.1,0.7,1.2;upper=1.2;upper=1.5,mubiao=1.1;目前的基线
            # ExpInfo(1, "PID_ALPHA_PST_AB_V2d1", "pst", ["1","2","39fb6bdb2d564d2982429abd23b2f62d"], [1.15,1.15,1.15], 1368, lower=[0.7,0.7,0.7], upper=[1.2,1.4,1.2]), #实验1，针对不同的广告位设置不同的上下限
            # ExpInfo(1, "PID_ALPHA_PST_AB_V2d2", "flowPst", ["1","2","39fb6bdb2d564d2982429abd23b2f62d"], [1.15,1.15,1.15], 1614, lower=[0.7,0.7,0.7], upper=[1.2,1.4,1.2]), #实验2，针对不同的广告位设置不同的上下限
            ExpInfo(1, "PID_ALPHA_PST_AB_V2d3", "flowPst", ["1","2","39fb6bdb2d564d2982429abd23b2f62d"], [1.2,1.15,1.15], 1614, lower=[0.7,0.7,0.7], upper=[1.2,1.4,1.2]), #实验3，信息流风控目标值调到1.2
            #ExpInfo("PID_FEEDS_ALPHAV2", "pst", 1.2, 1368)
        #     ExpInfo(2, "PID_FLOW_AB_V1", "flow", 1.1, 1377, lower=0.9, upper=1.2),
            #ExpInfo(3, "PID_FLOW_AB_V1", "compaigntype", 1.2, 1414, upper=1.2, lower=0.1),
            #ExpInfo(4, "PID_B_ALPHAV2", "compaigntype", 1.2, 1414, upper=1.2, lower=0.5),
            #ExpInfo(5, "PID_ALPHA_PST_AB_V1", "compaigntype", 1.2, 1414, upper=1.2, lower=0.5),
            #ExpInfo(6, "PID_FLOW_DSPTYPE_AB_V1", "compaigntype", 1.2, 1414, upper=1.2, lower=0.5),
        #     ExpInfo(7, "PID_FLOW_DSPTYPE_AB_V1", "flow", 1.2, 1369,lower=0.6,upper=3.0),
        #     ExpInfo(8, "PID_FLOW_AB_V2", "pst1", 1.2, 1421, lower=0.6,upper=3.0),
            #ExpInfo(9, "PID_FLOW_AB_V2", "compaigntype", 1.2, 1414, lower=0.5,upper=1.2),
        #     ExpInfo(10, "PID_ALPHA_PST_AB_V1", "pkg", 1.3, 1423, lower=0.5,upper=0.5),
            #ExpInfo(11, "PID_ALPHA_PST_AB_V1", "pkg", 1.0, 1425, lower=0.8,upper=1.2)
        #     ExpInfo(11, "PID_ALPHA_PST_AB_V1", "pkg", 1.0, 1425, lower=0.5,upper=0.5)
]
new_page_black_pst_list = set([])
#new_page_black_pst_list = set(["df79f631ffcf408297b674b25d858f7c", "c3a0dcaf6b4a4d29a9b7e50629e518fc", "a1f0f6d9352e4c809de4ad06fd17ff35", "5bb6ed5de1a24c59a2d106314ffafc9b", "fbae26fd6c3a40989d945fe15f25fba2", "cb783b3e5d054a9a8170734797c3e476", "688d52fad6994fb686189246bfce8592", "86f39c4b111941dfb33446a7278e1733","04faec160b1f4ed8bbb273785661c88e", "2970c6f830104c55b943eff3594271d9", "b08da09318cc4756ba1aea3b6609c662", "9ac4b2dbf2f44701ba891a3777022449", "5ae15cc1edfe4867b46127d455347184", "28170fc356fb402aa7b1d1b7078d0bae", "5fe08471194c413b8f59a52c199f423e", "2816ea183ad745b0a6a74e7a53888659", "ad1a30ba9c33477b8abab60d13b2b638"])
