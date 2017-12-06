#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/23 18:35
# @Author  : Zoe
# @Describe: 推荐第三步：对于candidates各策略产生的题目进行融合过滤
# @File    : mix.py
# @Software: PyCharm Community Edition
import random
import pandas as pd

def cross_mix(db_eval, db_study, suid, *args):
    '''
     -------------------交叉调和---------------------
     @功能: 根据多个策略选择的题目进行融合
     输入: 几种策略筛选出的题目DataFrame
        |  questionuid   |   strategy   |

     输出: 融合过滤得到的100道题目

    过滤条件：
         1.过滤掉已经推荐并接受过的结果
         2.优先CF选择出的题目
         3.各个策略按照比例各选择一部分
     -----------------------------------------------------------------
    '''
    if len(args) == 0:
        print("#" * 20 + 'ERROR' + '#' * 20)
        print('No Strategy selected!!!')
        return None

    #step1: Merge，根据知识点选出的题目做融合，去除其他策略选择出的不属于当前知识点的题目
    df_questionuid = args[0]
    for key in range(1, len(args)):
        df_questionuid = pd.merge(df_questionuid, args[key], how='left', on='questionuid')
    df_questionuid = df_questionuid.fillna(0)
    ##step2: 去除该学生已经推荐并接受的题目
    #sql=''

    #step3: 交叉调和
    #首先过滤三种策略选中的题目
    df_questionuid_select = df_questionuid[(df_questionuid['strategy_cf_user'] == 1)
                                           & (df_questionuid['strategy_highlevel'] == 1)]

    if len(df_questionuid_select) > 100:
        df_questionuid_select = df_questionuid_select.sample(n=100)
    #其次过滤两种策略选中的题目
    elif len(df_questionuid_select) < 100:
        df_questionuid.drop(list(df_questionuid_select.index))
        df_questionuid_pre_select = df_questionuid[(df_questionuid['strategy_cf_user'] == 1)
                                                   | (df_questionuid['strategy_highlevel'] == 1)]
        if len(df_questionuid_pre_select) > 100-len(df_questionuid_select):
            df_questionuid_pre_select = df_questionuid_pre_select.sample\
                (n=100-len(df_questionuid_select))
        df_questionuid_select = pd.concat([df_questionuid_select, df_questionuid_pre_select])

        #最后从剩下的题目中随机选择
        if len(df_questionuid_select) < 100:
            df_questionuid.drop(list(df_questionuid_pre_select.index))
            df_questionuid_pre_select = df_questionuid.sample(n=min(100-len(df_questionuid_select),len(df_questionuid)))
            df_questionuid_select = pd.concat([df_questionuid_select, df_questionuid_pre_select])
    return df_questionuid_select

def test_unit(suid, df_questionuid_kpointid, df_questionuid_cf_user, df_questionuid_highlevel):
    """
    单元测试
    :return: 
    """
    import time
    import database.study_pool as study_pool
    import database.eval_pool as eval_pool

#    suid = "932532079593361408"
#    df_questionuid_kpointid = pd.DataFrame({'questionuid': [
#        b'1-4930064', b'1-4939169', b'1-4939485', b'1-4939495', b'1-4939528', b'1-4939606',
#        b'1-4939761', b'1-4939781', b'1-4939833', b'1-4939950', b'1-4940684', b'1-4942037',
#        b'1-4942458', b'1-4942461', b'1-4942561', b'1-4942566', b'1-4942710', b'1-4944036',
#        b'1-4945230', b'1-4946095', b'1-4946133', b'1-4946817', b'1-4951696', b'1-4958236',
#        b'1-4963286', b'1-4963366', b'1-4963484', b'1-4963542', b'1-4963611', b'1-4968157',
#        b'1-4968173', b'1-4968235', b'1-4969122', b'1-4969508', b'1-4969619', b'1-4969649',
#        b'1-4970546', b'1-4970547', b'1-4970975', b'1-4970987', b'1-4970988', b'1-4971109',
#        b'1-4971112', b'1-4971258', b'1-4971268', b'1-4971269', b'1-4971270', b'1-4971272',
#        b'1-4971356', b'1-4971455'], 'strategy_kpointid': [1] * 50})
#
#    df_questionuid_cf_user = pd.DataFrame({'questionuid': [
#        b'1-4930064', b'1-4939169', b'1-4939485', b'1-4939495', b'1-4939528', b'1-4939606',
#        b'1-4939761', b'1-4939781', b'1-123456'], 'strategy_cf_user': [1] * 9})
#    df_questionuid_highlevel = pd.DataFrame({'questionuid': [
#        b'1-4951696', b'1-4958236', b'1-4963286', b'1-4963366', b'1-4963484', b'1-4963542',
#        b'1-4963611', b'1-123456'], 'strategy_highlevel': [1] * 8})

    with eval_pool.EvalConnectionPool() as db_eval:
        with study_pool.StudyConnectionPool() as db_study:
            starttime = time.time()
            # df_questionuid = pd.merge(df_questionuid_kpointid, df_questionuid_cf_user,
            #                           how='left', on='questionuid')
            # df_questionuid = pd.merge(df_questionuid, df_questionuid_highlevel,
            #                           how='left', on='questionuid')
            # df_questionuid = df_questionuid.fillna(0)

            args = [df_questionuid_kpointid, df_questionuid_cf_user, df_questionuid_highlevel]
            df_questionuid = cross_mix(db_eval, db_study, suid, *args)
    print("Cost time:",time.time()-starttime)
    return df_questionuid

if __name__ == '__main__':
    df_questionuid = test_unit(suid, df_questionuid_kpointid, df_questionuid_cf_user, df_questionuid_highlevel)