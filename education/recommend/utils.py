#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 10:52
# @Author  : Zoe
# @Site    : 
# @File    : utils.py
# @Software: PyCharm Community Edition

import random
import datetime

def get_uid_by_time():
    """ 获得基于时间的唯一性id   

    Args:
        offset  随机  0-100间

    Returns:
        唯一性id  长整型 19位  
    """
    ctime = datetime.datetime.now()
    luid = 10000000000000000 * (ctime.year - 2010)

    cstr = ctime.strftime('%m%d%H%M%S%f')  # 月-毫秒
    luid = luid + int(cstr)  # 毫秒有6位

    #   3位毫秒+5位随机数
    rint = random.randint(0, 9999)
    finaluid = luid * 100 + rint

    return finaluid

if __name__ == '__main__':
    for i in range(1000):
        print(get_uid_by_time())
