'''
Description:
Author: zhaoshanshan
Date: 2023-04-17 19:04:39
'''
import sys
#importlib.reload(sys)


class ExpInfo(object):
    def __init__(self, exp_id, exp_name, dim_info, dim_value, target_ratio, druid1_id, field_suffix="_all_query_control", lower=0.8, upper=1.2):
        self.exp_id = exp_id
        self.exp_name = exp_name
        self.dim_info = dim_info
        self.dim_value = dim_value
        self.target_ratio = target_ratio
        self.druid1_id = druid1_id
        self.field_suffix = field_suffix
        self.lower = lower
        self.upper = upper
