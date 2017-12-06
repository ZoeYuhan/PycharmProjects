#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/16 17:45
# @Author  : Zoe
# @Describe: 推荐第一步：产生知识点
# @File    : calc_kpoint.py
# @Software: PyCharm Community Edition
import pandas as pd

def calc_kpoint_from_exam(db_eval, db_study, formuid):
    """
     -------------------根据考试ID产生知识点---------------------
     @功能:  计算知识点
     输入: formuid (string)
     输出: df_kpointid (DataFrame)

     主要依据过滤，过滤条件：
        本次考试内部涉及到的所有知识点
     ------------------------------------------------------
    """

    #计算考试涉及的题目信息(formuid 为已经转换后的)
    df_topic = pd.io.sql.read_sql("SELECT DISTINCT questionuid FROM student_score_question "
                                  "WHERE formuid=%s", db_eval.conn, params=[formuid])
    if len(df_topic) == 0:
        print("#"*20+'ERROR'+'#'*20)
        print('No questionuid in student_score_question with formuid=%s'%formuid)
        return None
    #计算知识点
    sql = "SELECT questionuid,itemid FROM question_score WHERE questionuid IN ({0})".format(
        ','.join(['%s'] * len(df_topic)))
    df_kpointid = pd.io.sql.read_sql(sql, con=db_study.conn, params=list(df_topic["questionuid"]))
    df_kpointid.dropna(axis=0)
    df_kpointid = df_kpointid[df_kpointid['itemid'] != b'']

    if len(df_kpointid) == 0:
        print("#" * 20 + 'ERROR' + '#' * 20)
        print('No kpointid with formuid=%s' % formuid)
        return None
    return df_kpointid


def calc_kpoint_from_suid(db_eval, db_study, suid, subject):
    """
     -------------------根据学生产生知识点---------------------
     @功能:  计算知识点
     输入: suid (string)
     输出: df_kpointid (DataFrame)

     主要依据过滤，过滤条件：
        学生薄弱知识点 （低于平均水平 范围：学校）
        重难点知识点（ques_tag_distribution）
     ------------------------------------------------------
    """
    #计算属于同一学校的学生评估的平均值
    sql = 'SELECT studentid, kpointid, eval ' \
          'FROM student_eval_kpoint ' \
          'WHERE studentid in (SELECT studentid ' \
                               'FROM student_school ' \
                               'WHERE schoolid=(SELECT schoolid ' \
                                                'FROM student_school ' \
										        'WHERE studentid=%s)) '\
                                                'AND subject=%s AND inuse=1;'

    df_eval = pd.io.sql.read_sql(sql, db_eval.conn, params=[suid, subject])
    df_eval = df_eval.pivot(index='studentid', columns='kpointid', values='eval')
    df_suid_eval = df_eval.ix[bytes(suid, encoding='utf-8')]
    df_mean = df_eval.mean()
    kpointid = list(df_suid_eval[df_suid_eval < df_mean].index)
    return pd.DataFrame(columns=['itemid'], data=kpointid)


def calc_kpoint_from_ques(kpointid):
    """
     -------------------根据请求---------------------
     @功能:  计算知识点
     输入: kpointid (list)
     输出: df_kpointid (DataFrame)

     指定知识点
     ------------------------------------------------
    """
    return pd.DataFrame(columns=['itemid'], data=kpointid)


def test_unit():
    import time
    from database import study_pool, eval_pool

    #calc_kpoint_from_exam测试参数
    formuid = '932534884970369024'
    #calc_kpoint_from_suid测试参数
    suid = '932532079593361408'
    subject = '8'
    with eval_pool.EvalConnectionPool() as db_eval:
        with study_pool.StudyConnectionPool() as db_study:
            starttime = time.time()
            #df_kpointid = calc_kpoint_from_exam(db_eval, db_study, formuid)
            df_kpointid = calc_kpoint_from_suid(db_eval, db_study, suid, subject)
            print(df_kpointid)
            print('Costs time:', time.time()-starttime)

if __name__ == '__main__':
    test_unit()
