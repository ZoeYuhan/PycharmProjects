#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/16 18:20
# @Author  : Zoe
# @Describe: 推荐第二步：根据多种策略选择题目
# @File    : candidates.py
# @Software: PyCharm Community Edition

import numpy as np
import pandas as pd

# #策略标签
# KPOINT_STRATEGY=1
# CF_USER_STRATEGY=2
# HIGH_LEVEL_STRATEGY=3
NUM_PER_KPOINT=100

def candidate_kpoint(db_eval, db_study, suid, gradeid, df_kpoint, schoolid=None,
                     questiontype=None, old_thread=5):
    """
    -------------------根据知识点对每个学生产生候选集---------------------
    @功能:  计算知识点候选集
    输入:  suid(string), gradeid(string), df_kpointid(DataFrame),
          schoolid(string), questiontype(string or list), old_thread(int)
    输出: 
    主要依据过滤，过滤条件：
    1）知识点（‘question_score’ 过滤itemid 得到 questionuid）
    2）学科（subjectid）
    2）学生所在年级 （gradeid）
    3）校本题库 （school_id & is_private）
    4）难度 （difficulty）
    5）题型   （questiontype）
    6）重难点  考试类型决定（repo_type & exam_type &hot_value）
    7) 题目过老 （updatetime>5年）
    # 8）是否推荐过:
        ‘student_recommend_result_detail’过滤studentid得到resultid ‘student_recommend_result’ 学生ID
    -----------------------------------------------------------------
    """

    #筛选符合知识点和年级的题目
    sql = 'SELECT questionuid ' \
          'FROM question_info '\
          'WHERE questionuid in (SELECT questionuid ' \
                                 'FROM question_score qs ' \
                                 'WHERE qs.itemid in ({0})) '\
          'AND gradeid=%s'.format(",".join(["%s"]*len(df_kpoint)))
    param = list(df_kpoint['itemid'])+[gradeid]
    #筛选校本题库
    if schoolid is not None:
        sql = sql+' AND school_id=%s'
        param = param+[schoolid]
    # #筛选题目难度在XX范围
    # if difficulty is not None:
    #     if isinstance(difficulty, list):
    #         sql = sql+' and difficulty between %s and %s'
    #         param = param+difficulty
    #     else:
    #         print("#" * 20 + 'ERROR' + '#' * 20)
    #         print('Diffuculty type is not list')

    #筛选题目类型为XX
    if questiontype is not None:
        if isinstance(questiontype, str):
            questiontype = [questiontype]
        sql = sql+' AND questiontype IN ({0})'.format(",".join(["%s"]*len(questiontype)))
        param = param+list(questiontype)
    #筛选重难点题目：
#     if hot_value is not None:

    # #筛选过老的题目：
    sql = sql+' AND date_sub(curdate(), INTERVAL %s YEAR) <= date(`updatetime`)'
    param = param+[old_thread]

    #筛选推荐过的试题
    sql2 = 'SELECT questionuid FROM student_recommend_result_detail rd WHERE rd.resultid IN ' \
           '(SELECT resultid FROM student_recommend_result r WHERE r.studentid=%s)' \
           'AND selected=1 AND kpoint IN ({0})'.format(",".join(["%s"]*len(df_kpoint)))
    df_recommended_questionid = pd.io.sql.read_sql(sql2, db_eval.conn,
                                                   params=[suid]+list(df_kpoint['itemid']))
    if len(df_recommended_questionid) != 0:
        sql = sql+' EXCEPT questionuid IN ({0})'.format(",".join(["%s"]
                                                                 *len(df_recommended_questionid)))

    #筛选知识点范围，解决超纲问题

    #过滤掉相似题目

    #限制candidate 题目数为 知识点数*NUM_PER_KPOINT
    sql = sql+' LIMIT %s'%(len(df_kpoint)*NUM_PER_KPOINT)
    param = param + list(df_recommended_questionid['questionuid'])
    df_questionuid_kpoint = pd.io.sql.read_sql(sql, db_study.conn, params=param)

    #打策略标签
    df_questionuid_kpoint.insert(df_questionuid_kpoint.shape[1], column='strategy_kpoint',
                                    value=[1]*len(df_questionuid_kpoint))

    if len(df_questionuid_kpoint) == 0:
        print("#" * 20 + 'ERROR' + '#' * 20)
        print('No quetions meet condition!!!')
        return None
    return df_questionuid_kpoint


def get_group_users(db_eval, suid, df_kpoint, group='schoolid'):
    """
     -------------------找到群体用户---------------------
     @功能:  根据suid所在班级/年级/学校等找到群体用户
     输入: suid(string)， group(string)
     输出: user(DataFrame)
              | kpointid | studentid | eval |   
     同一group（'class', 'grade', 'school'）的学生评估结果
     -----------------------------------------------------------------
    """
    if group not in ['classid', 'gradeid', 'schoolid']:
        print("#" * 20 + 'ERROR' + '#' * 20)
        print('Group must in "["classid","gradeid","schoolid"]" , set default group="classid"!!')
        group = 'schoolid'
    sql = 'SELECT distinct '+group+' FROM student_score WHERE studentid=%s'
    groupid = db_eval.getOne(sql, param=suid)[0]

    sql = "SELECT studentid,kpointid,eval FROM student_eval_kpoint WHERE studentid IN " \
          "(SELECT DISTINCT studentid FROM student_score WHERE %s=%s) "\
          %(group, str(groupid, encoding='utf-8'))
    sql = sql+"AND kpointid IN ({0}) AND inuse=1".format(",".join(["%s"]*len(df_kpoint)))
    df_users = pd.io.sql.read_sql(sql, db_eval.conn, params=list(df_kpoint['itemid']))
    df_users = df_users.pivot(index='kpointid', columns='studentid', values='eval')
    return df_users


def eucldist_vectorized(vector1, vector2):
    """
    -------------------欧氏距离-------------------------
     @功能:  计算两个vector的欧式距离
     输入: vector1, vector2（list）
     输出: L2距离（float） 
     
     --------------------------------------------------
    """
    return np.linalg.norm(np.array(vector1) - np.array(vector2))

def cosdist_vectorized(vector1, vector2):
    """
    -------------------cosine距离-------------------------
     @功能:  计算两个vector的cosine距离
     输入: vector1, vector2（list）
     输出: cosine距离（float） 
     
     --------------------------------------------------
    """
    return np.dot(vector1, vector2)/(np.linalg.norm(vector1)*np.linalg.norm(vector2))

def topk_user(suid, df_users, k):
    """
    -------------------TOPK个相似的学生-------------------------
     @功能:  计算与suid相似的前k个学生
     输入: suid（string）， df_users(DataFrame)
     输出: topk_users(list) 
     
     --------------------------------------------------
    """
    dist = {}
    for user in df_users.columns:
        if bytes(suid, encoding='utf-8') == user:
            continue
        dist[user] = cosdist_vectorized(list(df_users[bytes(suid, encoding='utf-8')]),
                                        list(df_users[user]))

    dist = sorted(dist.items(), key=lambda item: item[1], reverse=True)
    topk_users = []
    for i in range(k):
        topk_users.append(dist[i][0])
    return topk_users


def candidate_cf_user(db_eval, db_study, suid, df_kpoint, group='schoolid', k=10):
    """
     -------------------根据知识点评估结果进行cf_User---------------------
     @功能:  CF_User计算候选集
     输入: suid(string)，df_kpoint(DataFrame), group(string), k(int)
     输出: 相似学生推荐并接受的试题

    过滤条件:
     TOPK个相似的学生做过的题目
     -----------------------------------------------------------------
    """
#    with eval_pool.EvalConnectionPool() as db_eval:
    #获得该学生所在group所有学生及知识点评估结果（N个学生*M个知识点）
    users = get_group_users(db_eval, suid, df_kpoint, group)
    #计算TOp-K个相似的学生
    topk_suid = topk_user(suid, users, k)
    if len(topk_suid)==0:
        print("No questionuid selected in CF_User strategy!")
        return pd.DataFrame()
    #找到Topk相似的学生推荐过并选定的题目
    # sql = 'SELECT questionuid FROM student_recommend_result_detail WHERE resultid IN (' \
    #       'SELECT resultid FROM student_recommend_result WHERE studentid IN ' \
    #       '({0}) AND selected=1)'.format(",".join(["%s"]*len(topk_suid)))
    #找到TOPK相似的学生做过的题目
    sql = 'SELECT DISTINCT questionuid FROM student_score_question WHERE studentid ' \
          'IN ({0})'.format(",".join(["%s"]*len(topk_suid)))
    df_questionuid_cf = pd.io.sql.read_sql(sql, db_eval.conn, params=topk_suid)

    #打标签
    df_questionuid_cf.insert(loc=df_questionuid_cf.shape[1], column='strategy_cf_user',
                             value=[1]*len(df_questionuid_cf))

    return df_questionuid_cf


def candidate_high_level(db_eval, db_study, suid, df_kpoint, group='schoolid', k=10):
    """
     -------------------根据知识点评估结果找到高一级学生---------------------
     @功能:  cf_User计算候选集
     输入: suid，Kpoint
     输出: 高一级学生推荐并接受的试题

     主要依据过滤，过滤条件：
         按照当前选定的知识点掌握程度和排序，比suid高的K个学生
     -----------------------------------------------------------------
    """
    # 获得该学生所在group所有学生及知识点评估结果（N个学生*M个知识点）
    users = get_group_users(db_eval, suid, df_kpoint, group)
    #所有知识点掌握程度求和
    user_sum = users.sum(axis=0)
    user_sum = user_sum.sort_values(ascending=False)
    indexs = list(user_sum.index)
    arg_index = indexs.index(bytes(suid, encoding='utf-8'))
    #比suid掌握程度好的K各用户
    topk_suid = indexs[max(arg_index-k, 0):arg_index]
    if len(topk_suid)==0:
        print("No questionuid selected in highlevel strategy!")
        return pd.DataFrame()
#    #找到topk学生推荐过并选定的题目
#    sql = 'select questionuid from student_recommend_result_detail where resultid in (' \
#        'select resultid from student_recommend_result where studentid in ' \
#        '({0}) and selected=1)'.format(",".join(["%s"]*len(topk_suid)))
    #找到TOPK相似的学生做过的题目
    sql = 'SELECT DISTINCT questionuid FROM student_score_question WHERE studentid ' \
          'IN ({0})'.format(",".join(["%s"]*len(topk_suid)))
    df_questionuid_highlevel = pd.io.sql.read_sql(sql, db_eval.conn, params=topk_suid)

    # 打标签
    df_questionuid_highlevel.insert(loc=df_questionuid_highlevel.shape[1],
                                    column='strategy_highlevel',
                                    value=[1] * len(df_questionuid_highlevel))
    return df_questionuid_highlevel

def test_unit():
    """
    单元测试
    """
    import time
    from database import study_pool, eval_pool
    # import student

    starttime = time.time()
    with eval_pool.EvalConnectionPool() as db_eval:
        with study_pool.StudyConnectionPool() as db_study:
            suid = "932532079593361408"
            # kpointid = ['25009', '25019', '25044', '25052', '25059', '25096', '25134', '25162',
            #             '25171', '25179', '25182', '25220', '25244', '25279', '25283', '25327',
            #             '25384', '25407', '25413', '25415', '25430']
            kpointid = ["25176", "25487", "25026", "25038", "25037", "25181", "25179", "25166",
                        "25172", "25175", "25189", "25159", "25150", "25182", "25162", "25183"]
            df_kpoint = pd.DataFrame(columns=['itemid'], data=kpointid)
            #gradeid= student.get_student_info(db_eval,suid)['gradeid']
            gradeid = 2
            df_questionuid_kpointid = candidate_kpoint(db_eval, db_study, suid, gradeid,
                                                       df_kpoint, schoolid=None,
                                                       questiontype=None)
            df_questionuid_cf_user = candidate_cf_user(db_eval, db_study, suid, df_kpoint, group='classid')
            df_questionuid_highlevel = candidate_high_level(db_eval, db_study, suid, df_kpoint, group='classid')
    print("Based kpoint:", len(df_questionuid_kpointid))
    print("Based cf:", len(df_questionuid_cf_user))
    print("Based high_level:", len(df_questionuid_highlevel))
    print('Cost time:', time.time() - starttime)
    return df_questionuid_kpointid, df_questionuid_cf_user, df_questionuid_highlevel

if __name__ == '__main__':
    df_questionuid_kpointid, df_questionuid_cf_user, df_questionuid_highlevel = test_unit()
