#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/18 17:02
# @Author  : Zoe
# @Site    : 
# @File    : mysql_pool.py
# @Software: PyCharm Community Edition

import pymysql
from DBUtils.PooledDB import PooledDB
from database import study_config as Config

'''
@功能：study数据库连接池
'''
class StudyConnectionPool(object):
    __pool = None
    def __enter__(self):
        self.conn = self.getConn()
        self.cursor = self.conn.cursor()
        # print("Study数据库创建con和cursor")
        return self

    def getConn(self):
        if self.__pool is None:
            self.__pool = PooledDB(creator=pymysql, mincached=Config.DB_MIN_CACHED , maxcached=Config.DB_MAX_CACHED,
                                   maxshared=Config.DB_MAX_SHARED, maxconnections=Config.DB_MAX_CONNECYIONS,
                                   blocking=Config.DB_BLOCKING, maxusage=Config.DB_MAX_USAGE,
                                   setsession=Config.DB_SET_SESSION,
                                   host=Config.DB_TEST_HOST , port=Config.DB_TEST_PORT ,
                                   user=Config.DB_TEST_USER , passwd=Config.DB_TEST_PASSWORD ,
                                   db=Config.DB_TEST_DBNAME , use_unicode=False, charset=Config.DB_CHARSET)

        return self.__pool.connection()

    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self.cursor.execute(sql)
        else:
            count = self.cursor.execute(sql, param)
        if count > 0:
            result = self.cursor.fetchone()
        else:
            result = False
        return result

    def insertOne(self, sql, value):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """
        count=self.cursor.execute(sql, value)
        self.conn.commit()
        return count
    def insertMany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self.cursor.executemany(sql, values)
        self.conn.commit()
        return count

    """
    @summary: 释放连接池资源
    """
    def __exit__(self, type, value, trace):
        self.cursor.close()
        self.conn.close()

        # print ("Study连接池释放con和cursor")

'''
@功能：获取study数据库连接
'''
def getstudyConnection():
    return StudyConnectionPool()

if __name__ == '__main__':
    import pandas as pd
    with getstudyConnection() as db:
        try:
            sql="Select * from question_score where questionuid=1495699834402"
            df=pd.read_sql(sql,db.conn)
            print (df)
        except Exception as err:
            print(err)