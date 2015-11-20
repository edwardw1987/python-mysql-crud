#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:29:13
# @Last Modified by:   edward
# @Last Modified time: 2015-11-17 23:42:34
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

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        return self.close()

    def queryset(self):
        return QuerySet(self)


class QuerySet:

    """
    'QuerySet' stands to be receiving a iterable object containing dict-like object.
    """

    def __init__(self, iterable):
        self._resultset = iter(iterable)

    def _retrieve(self):
        if hasattr(self._resultset, '__enter__'):
            with self._resultset as _rs:
                for r in _rs:
                    yield r
        else:
            for r in self._resultset:
                yield r

    def __iter__(self):
        return self._retrieve()

    def groupby(self, fieldname):
        _dict = Storage()
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
        return tuple(i for i in islice(self, start, stop, step))


class Storage(dict):

    def __iter__(self):
        return iter(self.items())

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
if __name__ == '__main__':
    import doctest
    doctest.testmod()
