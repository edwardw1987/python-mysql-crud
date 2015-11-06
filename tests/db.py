#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:54:09
# @Last Modified by:   edward
# @Last Modified time: 2015-11-06 11:55:12
import sys
sys.path.append('../../')
from mydql import DataBase
db = DataBase(host='localhost', db='QGYM', user='root', passwd='123123')
def mydql():
    return db.dql()