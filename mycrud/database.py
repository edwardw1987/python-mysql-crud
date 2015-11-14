#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-07 14:51:48
# @Last Modified by:   edward
# @Last Modified time: 2015-11-14 09:28:01
__metaclass__ = type
from .utils import connect, Storage
from .crud import DQL, DML


class DataBase:

    def __init__(self, host, name, user, passwd):
        self.host = host
        self.name = name
        self.user = user
        self.passwd = passwd
        self.tables = Storage()
        self._init_db(host=host, db=name, user=user, passwd=passwd)

    def _init_db(self, **kwargs):
        mapping = self._init_mapping(**kwargs)
        for tblname, fl in mapping.items():
            self._init_table(tblname, fl)

    def _init_table(self, tblname, fields):
        self.tables[tblname] = Table(name=tblname, fields=fields)
 
    def _init_mapping(self, **kwargs):
        cursor = connect(**kwargs).cursor()
        cursor.execute('SHOW TABLES')
        tables = Storage()
        for r in cursor.iterator():
            for tbname in r.values():
                tables[tbname] = []
                break

        for key in tables:
            cursor.execute('DESC %s' % key)
            ls = tables[key]
            for r in cursor.iterator():
                ls.append(r['Field'])
            tables[key] = tuple(tables[key])
        return tables

    def GetTable(self, tblname):
        return self.tables[tblname]

    def IterTable(self, fieldname):
        for table in self.tables.values():
            if fieldname in table.fields:
                yield table

    def GetField(self, tblname, fieldname):
        return self.tables[tblname].fields[fieldname]

    def IterField(self, fieldname):
        for table in self.IterTable(fieldname):
            yield table.fields[fieldname]

    def dql(self):
        return DQL(self)

    def dml(self):
        return DML(self)

class Table:

    """
        represents a table in database
    """

    def __init__(self, name, fields, alias=''):
        self.name = name
        self.alias = alias
        self._init_fields(fields)

    def _init_fields(self, fields):
        self.fields = Storage()
        for fname in fields:
            self.fields[fname] = Field(tb=self, name=fname)

    def iterfields(self):
        for f in self.fields.values():
            yield f

    def iterfieldnames(self):
        for f in self.iterfields():
            yield (f.mutation or '%s.%s' % (self.alias or self.name, f.name))

    def __repr__(self):
        return '<type: %r, name: %r, alias: %r>' % (self.__class__.__name__, self.name, self.alias)

    def set_alias(self, alias):
        self.alias = alias

class Field:

    def __init__(self, tb, name):
        self.tb = tb
        self.name = name
        self._mutation = None

    @property
    def fullname(self):
        return '%s.%s' % (
            self.tb.alias or self.tb.name,
            self.name)

    @property
    def mutation(self):
        return self._mutation

    def date_format(self, fmt, alias=''):
        mut = 'DATE_FORMAT(%s, %r) AS %s' % (
            self.fullname,
            fmt,
            alias or self.name)
        self._mutation = mut
        return mut

    def group_concat(self, alias=''):
        mut = 'GROUP_CONCAT(%s) AS %s' % (
            self.fullname,
            alias or self.name)
        return mut
    
