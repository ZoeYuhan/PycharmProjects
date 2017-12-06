#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/21 10:48
# @Author  : Zoe
# @Site    : 
# @File    : Student.py
# @Software: PyCharm Community Edition
import pandas as pd
import numpy as np

'''

 -------------------学生信息---------------------
@功能:根据examid读取学生信息 student_score表中读入
 输入: suid（DataFrame） or list
 输出: df_stu_info
 	  index       schoolid   gradeid    classid
       suid    |      
       suid    |	    
------------------------------------------------------

'''
def get_student_info(db_eval,suid):
    sql="SELECT studentid,schoolid,gradeid,classid FROM student_score WHERE studentid IN ({0})".format(
            ', '.join(['%s'] * len(suid)))
    if isinstance(suid,pd.DataFrame):
        df_stu_info=pd.io.sql.read_sql(sql, con=db_eval.conn, params=list(suid["studentid"])).drop_duplicates()
    elif isinstance(suid,list):
        df_stu_info=pd.io.sql.read_sql(sql,con=db_eval.conn,params=suid).drop_duplicates()
    else:
        print("#" * 20 + 'ERROR' + '#' * 20)
        print('Suid type is not correct')
        return None
    df_stu_info.index=list(df_stu_info['studentid'])
    df_stu_info=df_stu_info.drop(['studentid'],axis=1)
    return df_stu_info



if __name__ == '__main__':
    # import study_pool
    import eval_pool
    suid=['932532079593361408','932532080629354496']
    df_suid=pd.DataFrame(data=suid,columns=['studentid'])
    with eval_pool.EvalConnectionPool() as db_eval:
    #     with study_pool.StudyConnectionPool() as db_study:
        df_stu_info=Get_Student_info(db_eval,df_suid)