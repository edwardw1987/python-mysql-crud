#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-10-09 13:41:39
# @Last Modified by:   edward
# @Last Modified time: 2015-11-14 12:24:02
__metaclass__ = type
from itertools import islice
from operator import itemgetter
from .utils import connect, dedupe
import sys
import re

class Joint:

    """
        'Joint' abstracts a class to represent the relation to each other between two joined-table.
    """

    def __init__(self, tb, rel):
        """
        tb: Table
        rel: str, 'a=b', 'a.id=b.id'
        """
        self.tb = tb
        self._init_rel(rel)

    def _init_rel(self, rel):
        self.rel = rel.strip()
        self.duplication = self.rel.split('=')[0].strip()
    
class Condition:

    def __init__(self, dictObj):
        self.dict = self._valid_dict(dictObj)
        self.token_mapping = {
            'eq': '= %s',
            'lt': '< %s',
            'lte': '<= %s',
            'gt': '> %s',
            'gte': '>= %s',
            'in': 'IN (%s)',
            'range': 'BETWEEN %s AND %s',
            'like': 'LIKE %s',
        }

    def _valid_dict(self, dictObj):
        """
            validate the value of items of dictObj
            if value is 'None' then filter item away
        """
        _filter_func = lambda p: False if p[-1] is None else True
        return dict(filter(_filter_func, dictObj.items()))

    def resolve(self, key):
        """
            'key__tail' --> ('key', 'tail')
            'key'       --> ('key', '')
        """
        ls = key.split('_' * 2)
        length = len(ls)
        if length == 1:
            res = (ls[0], '')
        else:
            res = ls[-2:]
        return tuple(res)

    def get_token(self, tail):
        """
            mapping token by tail, e.g. 'lt', 'eq', 'gt'...
        """
        token = self.token_mapping.get(tail) or self.token_mapping['eq']
        return token

    def get_fraction(self, key):
        """
            1. Get single sql-fraction such as 
               'id = 1','id IN (1,2,3)' or 'id >= 5'
            2. While value is of type of str or unicode, the new token will be used instead,
               e.g. city="上海", token '= %s' --> '= "%s"' 
            3. e.g. id__in=(1,) <==> WHERE id IN (1); val = (1,) --> '(1)'
               e.g. id__in=(1,2,3) <==> WHERE id IN (1,2,3); val = (1,2,3) --> '(1,2,3)'
        """
        ckey, tail = self.resolve(key)
        token = self.get_token(tail)
        value = self.dict[key]
        # ckey is equivalent to fieldname
        # access corresponding table by fieldname
        v = sys.version_info[0]
        if v == 2:
            string = basestring
        elif v == 3:
            string = str
        if isinstance(value, string):
            token = token % '"%s"'
        elif isinstance(value, (tuple, list)):
            if tail in ('in',):
                value = ','.join(str(i) for i in value)
        return '{key} {condition}'.format(key=ckey, condition=(token % value))

    def clause(self):
        """
            Get conditional clause joined with 'AND'
            e.g. ' AND a=1 AND b>2 AND c<10 ...'
        """
        return ' AND '.join(self.get_fraction(key) for key in self.dict.keys())

class QuerySet:

    def __init__(self, iterator):
        self.iterator = iterator

    def groupby(self, fieldname):
        _dict = {}
        _key = itemgetter(fieldname)
        for i in self.iterator:
            k = _key(i)
            _dict.setdefault(k, [])
            _dict[k].append(i)
        return _dict

    def orderby(self, field, desc=False):
        ls = list(self.iterator)
        ls.sort(key=itemgetter(field), reverse=desc)
        self.iterator = iter(ls)
        return self

    def distinct(self):
        pass

    def values(self, field, distinct=False):
        vg = (i[field] for i in self.iterator)
        if distinct is True:
            return tuple(dedupe(vg))
        else:
            return tuple(vg)

    def all(self):
        return tuple(self.iterator)

    def slice(self, start, stop, step=1):
        """
        start, stop, step
        """
        return tuple( i for i in islice(self.iterator, start, stop, step))

class SQL:

    def __init__(self, db):
        super(SQL, self).__init__()
        self.db = db
        self.reset()

    def reset(self):
        self._connection = None
        self._table = None

    def connect(self):
        conn = connect(
                host=self.db.host,
                db=self.db.name,
                user=self.db.user,
                passwd=self.db.passwd)
        self._connection = conn
        return conn
        
    def cursor(self):
        return self.connect().cursor()

    def commit(self):
        conn = self._connection
        return conn and conn.commit()

    def rollback(self):
        conn = self._connection
        return conn and conn.rollback()

    def _access_table(self, name):
        try:
            tb = getattr(self.db.tables, name)
        except AttributeError:
            raise ValueError('invalid table name %r' % name)
        else:
            return tb

    def table(self, tblname, alias=''):
        tb = self._access_table(tblname)
        tb.set_alias(alias)
        self._table = tb
        return self

    @property
    def fields(self):
        pass

    def write(self):
        pass

    @property
    def is_valid(self):
        try:
            self.validate()
        except ValueError:
            return False
        else:
            return True

    def validate(self, table=''):
        from .database import Table
        try:
            assert isinstance(self._table, Table)
        except AssertionError:
            raise ValueError('%r is not a valid instance of Table' % self._table)

INNER_JOIN = lambda tbl: ' INNER JOIN '.join(tbl)

class DQL(SQL):

    def __init__(self, db):
        super(DQL, self).__init__(db)
        self.reset()

    def reset(self):
        super(DQL, self).reset()
        self._distinct = False
        self._joints = []
        self._orderbys = []
        self._where = None
        self._limit = None

    def itertables(self):
        yield self._table
        for j in self._joints:
            yield j.tb

    @property
    def fields(self):
        fls = []
        for tb in self.itertables():
            fls.extend(tb.iterfieldnames())
        return tuple(fls)

    def write(self, *args, **kwargs):
        """
        fields:
            expect a iterable-object contains names of fields to select
            each name of field is expected to be in format 'table.field' or 'table_alias.field' if table_alias has been set. 
            if not given, defaults to 'self.fields' 
        excludes:
            expect a iterable-object contains names of fields to exclude among 'self.fields'
            the requirement of format as same as 'fields'
            if 'fields' argument is given, it would be ignored
        """
        # distinct
        # or
        # order by desc/asc
        # count
        # subquery
        # avg
        # Aggregation
        # group by
        # having
        # union
        # not
        super(DQL, self).validate()
        ks = kwargs
        SELECT     = 'SELECT'
        DISTINCT   = self._handle_distinct()
        FIELDS     = self._handle_fields(ks.get('fields'), ks.get('excludes'))
        FROM       = 'FROM'
        TABLES     = self._handle_tables(method=INNER_JOIN)
        WHERE      = self._handle_where()
        ORDER_BY   = self._handle_orderby()
        LIMIT      = self._handle_limit()
        components = [ k for k in (
            SELECT, DISTINCT, FIELDS, FROM, TABLES,
            WHERE, ORDER_BY, LIMIT) if k is not None]
        return ' '.join(components)

    def _handle_limit(self):
        return ('LIMIT %s,%s' % self._limit) if self._limit else None

    def _handle_where(self):
        return ('WHERE %s' % self._where) if self._where else None  

    def _handle_distinct(self):
        return 'DISTINCT' if self._distinct else None

    def _handle_orderby(self):
        obs = self._orderbys
        if obs == []:
            return
        else:
            ob = ', '.join(obs)
            return 'ORDER BY %s' % ob

    def _handle_tables(self, method):
        tbl = []
        for j in self._joints:
            f = '{name} AS {alias} ON {rel}' if bool(
                j.tb.alias) is True else '{name} ON {rel}'
            tbl.append(
                f.format(name=j.tb.name, alias=j.tb.alias, rel=j.rel))
        main_f = '{name} AS {alias}' if self._table.alias else '{name}'
        main = main_f.format(
            name=self._table.name, alias=self._table.alias)
        tbl.insert(0, main)
        return method(tbl)

    def _handle_fields(self, fields, excludes):
        allset = set(self.fields)
        exset  = set(excludes or [])
        inset  = set(fields or [])
        union  = allset | exset | inset
        try:
            assert allset == union
        except AssertionError:
            invalid = list(union - allset)[0]
            raise ValueError('%r is invalid filedname' % invalid) 
        if fields is None:
            _fields = ', '.join(allset - exset)
        else:
            _fields = ', '.join(inset or allset)
        return _fields

    @property
    def queryset(self):
        return self.query()

    def query(self, *args, **kwargs):
        cursor = self.cursor()
        cursor.execute(self.write(*args, **kwargs))
        return QuerySet(cursor.iterator())

    def queryone(self, *args, **kwargs):
        cursor = self.cursor()
        cursor.execute(self.write(*args, **kwargs))
        return cursor.fetchone()

    def inner_join(self, tblname, on, alias=''):
        tb = self._access_table(tblname)
        tb.set_alias(alias)
        self._joints.append(Joint(tb,on))
        return self

    @staticmethod
    def validate_field(field):
        """
        field str 
        2 fullname
        1 shortname
        """
        pattern = re.compile(r'[\w]+(?:[.])[\w]+')
        if pattern.match(field):
            if '.' in field:
                return 2
            else:
                return 1
        else:
            raise ValueError('invalid field name %r' % field)

    def where(self, dictObj={}, **kwargs):
        _dictObj = dictObj.copy()
        _dictObj.update(**kwargs)
        self._where = Condition(_dictObj).clause()
        return self

    def orderby(self, field, desc=False, key=None):
        """
        field: str, name of field
        defaults to order by field, if key is given then order by value key(field) returns
        """
        into_desc = lambda f:'%s DESC' % f 
        ob = key and key(field) or field 
        if desc is True : ob = into_desc(ob)
        self._orderbys.append(ob)
        return self

    def distinct(self):
        self._distinct = True
        return self

    def limit(self, startpos, count):
        self._limit = startpos, count
        return self


class DML(SQL):

    def __init__(self, db):
        super(DML, self).__init__(db)
        self.reset()

    def reset(self):
        super(DML, self).reset()
        self._value = None
        self._values = []
        self._where  = None

    @property
    def fields(self):
        fls = []
        if self._table is not None:
            fls.extend(self._table.iterfieldnames())
        return tuple(fls)

    def value(self, **kwargs):
        self._value = kwargs
        return self

    def delete(self):
        cursor = self.cursor()
        cursor.execute(self.write('delete'))

    def insert(self, dictObj={}, **kwargs):
        _dictObj = dictObj.copy()
        _dictObj.update(kwargs)
        self.value(**_dictObj)
        cursor = self.cursor()
        cursor.execute(self.write('insert'))
        return self
    # def values(self, values):
    #     """
    #     param 'values' expects for iterable-object contains dict 
    #     """
    #     for v in values:
    #         self._values.append(values)
    #     return self

    def update(self, dictObj={}, **kwargs):
        _dictObj = dictObj.copy()
        _dictObj.update(kwargs)
        self.value(**_dictObj)
        cursor = self.cursor()
        cursor.execute(self.write('update'))
        return self

    def where(self, dictObj={}, **kwargs):
        _do = dictObj.copy()
        _do.update(kwargs)
        self._where = Condition(_do).clause()
        return self

    def write(self, key):
        # to write sql for operation of `insert` or `update` or `delete`
        # key str `insert/create`, `update`, `delete`
        self.validate()
        DICT = dict(INSERT = 'INSERT INTO',
                    UPDATE = 'UPDATE',
                    DELETE = 'DELETE FROM',)
        try:
            _key = key.upper()
            KEYWD = DICT[_key]
        except KeyError:
            raise ValueError('unsupported type of %r' % _key)
        else:
            TABLE  = self._handle_table()
            VALUE  = self._handle_value()
            WHERE  = self._handle_where()
            # VALUES = self._handle_values()
            components = [k for k in (KEYWD, TABLE, VALUE, WHERE) if k is not None]
            return ' '.join(components)

    def _handle_table(self):
        return self._table.name

    def _handle_value(self):
        if self._value is None:
            return
        else:
            l = []
            f = '%s=%s'
            for k, v in self._value.items():
                if isinstance(v, str): f = '%s="%s"'
                l.append(f % (k, v))
            return 'SET %s' % (', '.join(l))

    def _handle_values(self):
        pass
        
    def _handle_where(self):
        return ('WHERE %s' % self._where) if self._where else None  
