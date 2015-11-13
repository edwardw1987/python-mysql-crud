#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:29:13
# @Last Modified by:   edward
# @Last Modified time: 2015-11-12 22:43:50
from pymysql.cursors import DictCursor 
from pymysql.connections import Connection 


def sortit(iterable, key=None, reverse=False, conv=iter):
    """
    An alternative to 'sorted' which returns a sorted-iterator instead of a list.
    """
    return conv(sorted(iterable, key=key, reverse=_rev))


def connect(**kwargs):
    """
    A wrapped function based on 'MySQLdb.connections.Connection' returns a 'Connection' instance.
    """
    kwargs['cursorclass'] = kwargs.pop('cursorclass', None) or DQLCursor
    kwargs['charset'] = kwargs.pop('charset', None) or 'utf8'
    return Connection(**kwargs)


def dedupe(items):
    seen = set()
    for item in items:
        if item not in seen:
            yield item
            seen.add(item)


class DQLCursor(DictCursor):

    def iterator(self):
        while 1:
            r = self.fetchone()
            if r is None:
                break
            else:
                yield r


class Storage(dict):

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError as K:
            raise AttributeError(k)

    def __repr__(self):
        return '<Storage ' + dict.__repr__(self) + '>'
