#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:54:09
# @Last Modified by:   edward
# @Last Modified time: 2015-11-12 22:39:12
import sys
sys.path.append('../')
from mycrud import DataBase
DB = DataBase(host='localhost', name='db', user='root', passwd='123123')
