#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:29:13
# @Last Modified by:   edward
# @Last Modified time: 2015-11-21 20:44:29

try:
    from pymysql.cursors import DictCursor
    from pymysql.connections import Connection
except ImportError:
    from MySQLdb.cursors import DictCursor
    from MySQLdb.connections import Connection
from operator import itemgetter
from itertools import islice
import sys
from copy import deepcopy as clone


def string_type():
    v = sys.version_info[0]
    if v == 2:
        _str = basestring
    elif v == 3:
        _str = str
    return _str
StringType = string_type()


def dq(s):
    """
    double-quote str object, e.g. 
    'a' --> '"a"'
    or apply str to non-str object 
    100 --> '100'
    """
    if isinstance(s, StringType):
        return '"%s"' % s.replace('"', '')
    else:
        return str(s)

        
def isnumberic(s):
    try:
        int(s)
    except ValueError:
        return False
    else:
        return True


def connect(**kwargs):
    """
    A wrapped function based on '.connections.Connection' returns a 'Connection' instance.
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


class Cursor(DictCursor):

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        return self.close()

    def queryset(self):
        return QuerySet(self)


class QuerySet:

    """
    'QuerySet' expects to receive iterable which containing dict-like objects.
    """

    def __init__(self, iterable):
        self._result_set = iter(iterable)

    def _retrieve(self):
        if hasattr(self._result_set, '__enter__'):
            with self._result_set as _rs:
                for r in _rs:
                    yield r
        else:
            for r in self._result_set:
                yield r

    def __iter__(self):
        return self._retrieve()

    def groupby(self, key):
        """
        Grouping dict-like objects by the given key
        to make up a dict-like object for finally return
        >>> years = [{'year':2015}, {'year':2014},{'year':2013}]
        >>> QuerySet(years).groupby('year')
        {2013: [{'year': 2013}], 2014: [{'year': 2014}], 2015: [{'year': 2015}]}
        """
        stg = Storage()
        key = itemgetter(key)
        for i in self:
            k = key(i)
            stg[k] = []
            stg[k].append(i)
        return stg

    def values(self, key, distinct=False):
        """
        Accessing the values of each dict-like objects by the given key
        to make up a tuple object for finally return
        ps: if distinct is given as True, then goes to remove the duplicated elements in order
        >>> months = [{'month':12},{'month':11},{'month':11}]
        >>> QuerySet(months).values('month')
        (12, 11, 11)
        >>> QuerySet(months).values('month', distinct=True)
        (12, 11)
        """
        vg = (i[key] for i in self)
        if distinct is True:
            return tuple(dedupe(vg))
        else:
            return tuple(vg)

    def all(self):
        """
        directly returns all dict-like objects in a tuple
        """
        return tuple(self)

    def slice(self, start, stop, step=1):
        """
        retrieving a range of the result-set which returns in a tuple
        """
        return tuple(i for i in islice(self, start, stop, step))


class _S:

    """
    >>> s = _S()
    >>> s[1] = 2
    >>> s[1] = 3
    >>> s[1]
    2
    >>> s.a = 4
    >>> s.a = 5
    >>> s.a
    4
    >>> s = _S()
    >>> s['a'] = 1
    >>> s['b'] = 2
    >>> s['c'] = 3
    >>> sorted(s, key=lambda x:x[-1])
    [('a', 1), ('b', 2), ('c', 3)]
    """

    def __iter__(self):
        return iter(self.__dict__.items())

    def __repr__(self):
        return repr(self.__dict__)

    def __setitem__(self, k, v):
        return self._add(k, v)

    def __getitem__(self, k):
        return self.__dict__[k]

    def __setattr__(self, k, v):
        return self._add(k, v)

    def _add(self, k, v):
        dt = self.__dict__
        if k not in dt:
            dt[k] = v


Storage = _S

if __name__ == '__main__':
    import doctest
    doctest.testmod()
