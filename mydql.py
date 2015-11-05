#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-10-09 13:41:39
# @Last Modified by:   edward
# @Last Modified time: 2015-11-05 18:36:18
__metaclass__ = type
from MySQLdb.cursors import DictCursor
from MySQLdb.connections import Connection
from itertools import islice
from operator import itemgetter

def sortit(iterable, key=None, reverse=False, conv=iter):
    """
    An alternative to 'sorted' which returns a sorted-iterator instead of a list.
    """
    return conv(sorted(iterable, key=key, reverse=reverse))


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
# ====================

class DQLCursor(DictCursor):
    def iterator(self):
        while 1:    
            r = self.fetchone()
            if r is None:
                break
            else:
                yield r

class Storage(dict):

    """
    A Storage object is like a dictionary except `obj.foo` can be used
    in addition to `obj['foo']`.

        >>> o = storage(a=1)
        >>> o.a
        1
        >>> o['a']
        1
        >>> o.a = 2
        >>> o['a']
        2
        >>> del o.a
        >>> o.a
        Traceback (most recent call last):
            ...
        AttributeError: 'a'

    """

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


class FieldStorage(set):
    pass


class DataBase:

    def __init__(self, host, db, user, passwd):
        self.host = host
        self.name = db
        self.user = user
        self.passwd = passwd
        self.tables = Storage()
        self._init_db(host=host, db=db, user=user, passwd=passwd)

    def _init_db(self, **kwargs):
        mapping = self._init_mapping(**kwargs)
        for tblname, fl in mapping.iteritems():
            self._init_table(tblname, fl)

    def _init_table(self, tblname, fields):
        self.tables[tblname] = Table(name=tblname, fields=fields)
 
    def _init_mapping(self, **kwargs):
        cursor = connect(**kwargs).cursor()
        cursor.execute('SHOW TABLES')
        tables = Storage()
        for tbname in (next(r.itervalues()) for r in cursor.iterator()):
            tables[tbname] = []
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
        for table in self.tables.itervalues():
            if fieldname in table.fields:
                yield table

    def GetField(self, tblname, fieldname):
        return self.tables[tblname].fields[fieldname]

    def IterField(self, fieldname):
        for table in self.IterTable(fieldname):
            yield table.fields[fieldname]

    def dql(self):
        return DQL(self)


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
        for f in self.fields.itervalues():
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

    def DateFormat(self, fmt, alias=''):
        mut = 'DATE_FORMAT(%s, %r) AS %s' % (
            self.fullname,
            fmt,
            alias or self.name)
        self._mutation = mut
        return mut

    def GroupConcat(self, alias=''):
        mut = 'GROUP_CONCAT(%s) AS %s' % (
            self.fullname,
            alias or self.name)
        return mut
    

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
        return dict(filter(_filter_func, dictObj.iteritems()))

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

        if isinstance(value, basestring):
            token = token % '"%s"'
            if isinstance(value, unicode):
                value = value.encode("utf-8")
        elif isinstance(value, (tuple, list)):
            if tail in ('in',):
                value = ','.join(str(i) for i in value)
        return '{key} {condition}'.format(key=ckey, condition=(token % value))

    def clause(self):
        """
            Get joined conditional clause with 'AND'
            e.g. ' AND a=1 AND b>2 AND c<10 ...'
        """
        return ' AND '.join(self.get_fraction(key) for key in self.dict.iterkeys())


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
        if bool(distinct) is True:
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

INNER_JOIN = lambda tbl: ' INNER JOIN '.join(tbl)


class DQL:

    """
        'DQL' is a simple extension-class based on MySQLdb, 
        which is intended to make convenient-api for satisfying regular DQL-demand.

    """

    def __repr__(self): return 'MyDQL@MySQLdb'

    def __init__(self, db):
        self.db = db
        self.maintable = None
        self._distinct = False
        self._joints = []
        self._groupby = None
        self._orderbys = []

    @property
    def fields(self):
        return self._get_fields()

    def cursor(self):
        return connect(
                host=self.db.host,
                db=self.db.name,
                user=self.db.user,
                passwd=self.db.passwd).cursor()

    def _get_fields(self):
        fls = []
        if self.maintable is not None:
            fls.extend(self.maintable.iterfieldnames())
            for j in self._joints:
                fls.extend(j.tb.iterfieldnames())
        return tuple(fls)

    def setmain(self, tblname, alias=''):
        # try:
        #     assert isinstance(_table, Table)
        # except AssertionError:
        #     raise TypeError("%r is not an instance of 'Table'" % _table)
        # else:
        tb = getattr(self.db.tables, tblname)
        tb.set_alias(alias)
        self.maintable = tb
        return self

    def get_dql(self, *args, **kwargs):
        """
        fields:
            expect a iterable-object contains names of fields to select
            if not given, defaults to 'self.fields' 
        excludes:
            expect a iterable-object contains names of fields to exclude among 'self.fields'
            if 'fields' argument is given, it would be ignored
        where:
            expect dict-object contains keyword-argument as fitering-condtions

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
        ks = kwargs
        SELECT     = 'SELECT'
        DISTINCT   = self._handle_distinct()
        FIELDS     = self._handle_fields(ks.get('fields'), ks.get('excludes'))
        FROM       = 'FROM'
        TABLES     = self._relate(INNER_JOIN)
        CONDITIONS = self._handle_conditions(ks.get('where'))
        WHERE      = 'WHERE' if CONDITIONS else None
        GROUP_BY   = self._handle_groupby()
        # HAVING     = 
        ORDER_BY   = self._handle_orderby()

        [SELECT, DISTINCT, FIELDS, FROM, TABLES, WHERE, CONDITIONS, GROUP_BY, HAVING, ORDER_BY, LIMIT]

        # ==============================
        #
        _where_clause = Condition(where).clause() if where else '1=1'
        _dql = _dql_format.format(
            distinct='DISTINCT ' if distinct else '',
            fields=_fields,
            tables=self._relate(INNER_JOIN),
            conditions=_where_clause,
        )
        return _dql

    def _handle_conditions(self, where):
        return Condition(where).clause() if where else None  

    def _handle_distinct(self):
        return 'DISTINCT' if self._distinct else None

    def _handle_groupby(self):
        if self._groupby is None:
            return 
        else:
            return 'GROUP BY %s' % self._groupby

    def _handle_orderby(self):
        obs = self._orderbys
        if obs == []:
            return
        else:
            ob = ', '.join(obs)
            return 'ORDER BY %s' % ob

    def _handle_fields(self, fields, excludes):
        if fields is None:
            _fields_set = set(self.fields)
            _fields = ', '.join(_fields_set - set(excludes or []))
        else:
            _fields = ', '.join(fields or self.fields)
        return _fields

    def create_view(self, name, *args, **kwargs):
        self.db.cursor().execute('CREATE OR REPLACE VIEW {name} AS {dql} '.format(
            name=name, dql=self.get_dql(*args, **kwargs)))
        _view = Table(db=self.db, name=name)
        setattr(self.db, name, _view)
        return _view

    @property
    def queryset(self):
        return self.query()

    def query(self, *args, **kwargs):
        cursor = self.cursor()
        cursor.execute(self.get_dql(*args, **kwargs))
        return QuerySet(cursor.iterator())

    def queryone(self, *args, **kwargs):
        cursor = self.cursor()
        cursor.execute(self.get_dql(*args, **kwargs))
        return cursor.fetchone()

    def inner_join(self, tblname, on, alias=''):
        tb = getattr(self.db.tables, tblname)
        tb.set_alias(alias)
        self._joints.append(Joint(tb,on))
        return self

    def groupby(self, field, key=None):
        """
        field: str, name of field
        defaults to group by field, if key is given then group by value key(field) returns
        """
        self._groupby = key and key(field) or field

    def orderby(self, field, desc=False, key=None):
        """
        field: str, name of field
        defaults to order by field, if key is given then order by value key(field) returns
        """
        into_desc = lambda f:'%s DESC' % f 
        ob = key and key(field) or field 
        if desc is True : ob = into_desc(ob)
        self._orderbys.append(ob)

    def distinct(self):
        self._distinct = True

    def _relate(self, method):
        tbl = []
        for j in self._joints:
            f = '{name} AS {alias} ON {rel}' if bool(
                j.tb.alias) is True else '{name} ON {rel}'
            tbl.append(
                f.format(name=j.tb.name, alias=j.tb.alias, rel=j.rel))
        main_f = '{name} AS {alias}' if self.maintable.alias else '{name}'
        main = main_f.format(
            name=self.maintable.name, alias=self.maintable.alias)

        tbl.insert(0, main)
        return method(tbl)


def main():
    db = DataBase(host='localhost', db='QGYM', user='root', passwd='123123')
    print db.dql().setmain('user_table').queryset.all()
if __name__ == '__main__':
    main()
