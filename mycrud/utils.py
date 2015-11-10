#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-06 11:29:13
# @Last Modified by:   python
# @Last Modified time: 2015-11-10 18:35:50
from MySQLdb.cursors import DictCursor
from MySQLdb.connections import Connection

def sortit(iterable, key=None, reverse=False, reverse_key=None, conv=iter):
    """
    An alternative to 'sorted' which returns a sorted-iterator instead of a list.
    'reverse_key' defaults to 'None' & expects for a function which returns Ture(1) or False(0)
    if 'reverse' is True, 'reverse_key' will be ignored.
    """
    if (reverse is False) and hasattr(reverse_key, '__call__'):
        _rev = reverse_key()
    else:
        _rev = reverse
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
        except KeyError, k:
            raise AttributeError, k

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError, k:
            raise AttributeError, k

    def __repr__(self):
        return '<Storage ' + dict.__repr__(self) + '>'
