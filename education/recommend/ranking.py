#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/28 12:19
# @Author  : Zoe
# @Describe: 推荐第四步：根据mix产生的100道题目做排序
# @File    : ranking.py
# @Software: PyCharm Community Edition

import pandas as pd
from  model.FTRL import FtrlClassifier
import numpy as np

def ques_type_encode(df_questionuid):
    
    indexs = [501, 502, 503, 504, 505, 506, 507]
    df_temp = pd.get_dummies(df_questionuid['questiontype'])
    for col in indexs:
        if col not in df_temp.columns:
            df_temp.loc[:,col] = 0
    df_temp = df_temp.ix[:,indexs]
    df_questionuid = pd.concat([df_questionuid,df_temp],axis=1)
    df_questionuid = df_questionuid.drop(['questiontype'],axis=1)
    
    return df_questionuid

def ranking_LR(db_eval,db_study,suid,df_kpoint,df_questionuid):
    '''
     ----------------------------排序--------------------------------
     @功能: 对选出的题目计算得分并排序
     输入:
    
     输出: 
    
    Feature： 17列
    1.题目知识点和学生掌握程度相关性        （连续值  0-1归一化）           1
        计算： sum（（1-学生知识点掌握程度）*题目包含知识点）             
    2.题目难度                          （连续值  0-1归一化）           1
    3.题目题型                          （类别  one-hot encode）       7
    4.题目包含知识点个数（题目综合程度）    （连续值 0-1归一化）             1
    5.校本题库                          （类别  one-hot ebcode）       1
    6.重难点程度                        （连续值 0-1归一化）             1
    7.推荐并接受                        （类别  one-hot ebcode）        1
    8.来自策略                          （类别  one-hot ebcode）       3
    9.题目过老                          （连续值  0-1归一化）            1 
      
     -----------------------------------------------------------------
    '''
    with eval_pool.EvalConnectionPool() as db_eval:
        with study_pool.StudyConnectionPool() as db_study:
            #step1：题目知识点和学生掌握程度相关性计算
            #获得学生的评估结果
            df_questionuid.index=list(df_questionuid['questionuid'])
            sql = 'SELECT studentid,kpointid,eval FROM student_eval_kpoint WHERE studentid=%s AND inuse=1 AND ' \
                  'kpointid IN ({0})'.format(','.join(['%s'] * len(df_kpoint)))
            df_suid_eval = pd.io.sql.read_sql(sql,db_eval.conn,params=[suid]+list(df_kpoint['itemid']))
            df_suid_eval = df_suid_eval.pivot(index='studentid', columns='kpointid', values='eval')
            # 获得题目的知识点
            sql = 'SELECT questionuid,itemid FROM question_score WHERE questionuid IN ({0})'.format(
                ','.join(['%s'] * len(df_questionuid)))
            df_questionuid_kpoint = pd.io.sql.read_sql(sql,db_study.conn,params=list(df_questionuid['questionuid']))
            df_questionuid_kpoint['data'] = 1
            df_questionuid_kpoint = df_questionuid_kpoint.pivot(index='questionuid', columns='itemid',values='data')
            df_questionuid_kpoint = df_questionuid_kpoint.fillna(0)
                    
            temp = pd.DataFrame(index=df_questionuid_kpoint.index, columns=df_suid_eval.columns,
                                data=list(df_suid_eval.values)*len(df_questionuid_kpoint))
            df_questionuid_suid_relation = (temp*df_questionuid_kpoint).fillna(0).sum(axis=1)
            df_questionuid['relation'] = df_questionuid_suid_relation
            #step2: 知识点数目
            df_kpoint_num = df_questionuid_kpoint.sum(axis=1)
            df_questionuid['kpoint_num'] = df_kpoint_num/df_kpoint_num.values.max()
            #step3: 读取题目其他属性(难度，题型，校本题库，更新时间)
            sql = 'SELECT questionuid,difficulty,questiontype,school_id,DATEDIFF(curdate(),updatetime) AS time FROM question_info WHERE ' \
                  'questionuid IN ({0})'.format(','.join(['%s'] * len(df_questionuid)))
            df_questionuid_info = pd.io.sql.read_sql(sql, db_study.conn, params=list(df_questionuid['questionuid']))
            df_questionuid_info.index = list(df_questionuid_info['questionuid'])
            df_questionuid['difficulty'] = df_questionuid_info['difficulty'].astype('float')
            df_questionuid['questiontype'] = df_questionuid_info['questiontype'].astype('int')
#            df_questionuid['school_id'] = df_questionuid_info['school_id']
            df_questionuid['time'] = df_questionuid_info['time'].astype('float')/df_questionuid_info['time'].values.max()
            #step4: 读取重难点题目及推荐并接受的题目
#            #重难点题目（需要新建ques_static_info表进行统计）
#            sql = 'SELECT questionuid,num FROM ques_static_info ' \
#                  'WHERE questionuid IN ({0});'.format(','.join(['%s'] * len(df_questionuid)))
#            df = pd.io.sql.read_sql(sql, db_eval.conn, params=list(df_questionuid['questionuid']))
            #推荐并接受的题目
            sql = 'SELECT questionuid,selected FROM student_recommend_result_detail ' \
                  'WHERE questionuid IN ({0})'.format(','.join(['%s'] * len(df_questionuid)))
            df = pd.io.sql.read_sql(sql, db_eval.conn, params=list(df_questionuid['questionuid']))
            if len(df) != 0:
                df.index = df['questionuid']
                df = df.drop(['questionuid'], axis=1)
                df_questionuid['selected'] = df['selected']

            #Feature
            if 'questionuid' in df_questionuid.columns:
                df_questionuid=df_questionuid.drop(['questionuid'],axis=1)
            df_questionuid = ques_type_encode(df_questionuid)

            #model
            logreg = FtrlClassifier(X=np.array(df_questionuid.values))
            y_hat = logreg.predict(np.array(df_questionuid.values))
            
            #排序
           df_questionuid['score']=y_hat
           df_rec=df_questionuid.sort_values(by='score',ascending=False)             

if __name__ == '__main__':
    import time
    from database import study_pool, eval_pool

    starttime = time.time()
    with eval_pool.EvalConnectionPool() as db_eval:
        with study_pool.StudyConnectionPool() as db_study:
#            suid = "932532079593361408"
#            kpointid = ["25176", "25487", "25026", "25038", "25037", "25181", "25179", "25166",
#                        "25172", "25175", "25189", "25159", "25150", "25182", "25162", "25183"]
#            df_kpoint = pd.DataFrame(columns=['itemid'], data=kpointid)
#            questionuid = [b'1-2941665', b'1-2946344', b'1-2956329', b'1-2978427', b'1-2985662',
#                           b'1-2987915', b'1-2990088', b'1-2996788', b'1-3000091', b'1-3000128',
#                           b'1-3002709', b'1-3003079', b'1-3004194', b'1-3009383', b'1-3013471',
#                           b'1-3038233', b'1-3044743', b'1-3055877', b'1-3058771', b'1-3059864']
#            df_questionuid = pd.DataFrame(columns=['questionuid'], data=questionuid)
          
            #step1：题目知识点和学生掌握程度相关性计算
            sql = 'SELECT studentid,kpointid,eval FROM student_eval_kpoint WHERE studentid=%s AND' \
                  ' inuse=1 AND kpointid IN ({0})'.format(','.join(['%s'] * len(df_kpoint)))
            df_suid_eval = pd.io.sql.read_sql(sql, db_eval.conn, params=[suid] + list(df_kpoint['itemid']))
            df_suid_eval = df_suid_eval.pivot(index='studentid', columns='kpointid', values='eval')
          
            sql = 'SELECT questionuid,itemid FROM question_score WHERE questionuid IN ({0})'.format(
        ','.join(['%s'] * len(df_questionuid)))
            df_questionuid_kpoint = pd.io.sql.read_sql(sql,db_study.conn,params=list(df_questionuid['questionuid']))
            df_questionuid_kpoint['data'] = 1
            df_questionuid_kpoint = df_questionuid_kpoint.pivot(index='questionuid', columns='itemid',values='data')
            df_questionuid_kpoint = df_questionuid_kpoint.fillna(0)
            
            temp=pd.DataFrame(index=df_questionuid_kpoint.index,columns=df_suid_eval.columns,
                              data=list(df_suid_eval.values)*len(df_questionuid_kpoint))    
            df_questionuid_suid_relation=(temp*df_questionuid_kpoint).fillna(0).sum(axis=1)