#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:29:13
# @Last Modified by:   edward
# @Last Modified time: 2015-11-16 16:48:00
try:
    from pymysql.cursors import SSDictCursor 
    from pymysql.connections import Connection 
except ImportError:
    from MySQLdb.cursors import SSDictCursor
    from MySQLdb.connections import Connection
from operator import itemgetter
from itertools import islice
import sys

def string_type():
    v = sys.version_info[0]
    if v == 2:
        _str = basestring
    elif v == 3:
        _str = str
    return _str
StringType = string_type()

def sortit(iterable, key=None, reverse=False, conv=iter):
    """
    An alternative to 'sorted' which returns a sorted-iterator instead of a list.
    """
    return conv(sorted(iterable, key=key, reverse=_rev))


def connect(**kwargs):
    """
    A wrapped function based on 'MySQLdb.connections.Connection' returns a 'Connection' instance.
    """
    kwargs['cursorclass'] = kwargs.pop('cursorclass', None) or Cursor
    kwargs['charset'] = kwargs.pop('charset', None) or 'utf8'
    return Connection(**kwargs)


def dedupe(items):
    seen = set()
    for item in items:
        if item not in seen:
            yield item
            seen.add(item)


class Cursor(SSDictCursor):

    def queryset(self):
        return QuerySet(self)

    def execute(self, *args, **kwargs):
        try:
            super(Cursor, self).execute(*args, **kwargs)
        except:
            raise ValueError(*args, **kwargs)

class QuerySet:
    """
        QuerySet receives a cursor
    """
    def __init__(self, cursor):
        self.cursor = cursor

    def _retrieve(self):
        for row in self.cursor:
            if row is None:
                self.cursor.close()
                break
            else:
                yield row

    def __iter__(self):
        return self._retrieve()

    def groupby(self, fieldname):
        _dict = {}
        _key = itemgetter(fieldname)
        for i in self:
            k = _key(i)
            _dict.setdefault(k, [])
            _dict[k].append(i)
        return _dict

    def values(self, field, distinct=False):
        vg = (i[field] for i in self)
        if distinct is True:
            return tuple(dedupe(vg))
        else:
            return tuple(vg)

    def all(self):
        return tuple(self)

    def slice(self, start, stop, step=1):
        """
        don't consume
        start, stop, step
        """
        return tuple( i for i in islice(self, start, stop, step))



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
