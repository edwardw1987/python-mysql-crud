#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:54:09
# @Last Modified by:   edward
# @Last Modified time: 2015-11-10 16:15:32
import sys
sys.path.append('../../')
from mycrud import DataBase
DB = DataBase(host='localhost', name='QGYM', user='root', passwd='123123')

for i in xrange(48,58):
    print chr(i)