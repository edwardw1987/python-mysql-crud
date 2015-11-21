#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-20 20:45:05
# @Last Modified by:   edward
# @Last Modified time: 2015-11-22 00:09:54
__metaclass__ = type
try:
    from utils import connect, StringType, clone, dq
except ImportError:
    from .utils import connect, StringType, clone, dq
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
    
# ====================
OPERATORS = {
    'eq': '=',
    'lt': '<',
    'lte': '<=',
    'gt': '>',
    'gte': '>=',
    'in': 'IN',
    'like': 'LIKE',
}

class Hack:
    """
    Hacking default behavior of signs
    """
    def __init__(self, key):
        self.key = key

    def __repr__(self):
        return repr(self.read())

    def set(self, sign, val):
        self.exp = ('%s {} %s'.format(sign)) % (self.key, val)
        return self

    def read(self):
        return getattr(self, 'group', getattr(self, 'exp', ''))

    def resolve(self):
        """
        >>> a = Hack('a'); b = Hack('b')
        >>> a.resolve()
        ()
        >>> a > 123; b < 456;
        >>> c = a & b 
        >>> a.resolve()
        ('a > 123',)
        >>> c.resolve()
        ('a > 123', 'b < 456')
        """
        return tuple(i for i in map(str.strip, re.split(r'AND|OR|\)|\(', self.read())) if i != '')

    def __and__(self, hack):
        """
        >>> a = Hack('a'); b = Hack('b')
        >>> a > 1; b < 2
        >>> c = a & b
        >>> c
        'a > 1 AND b < 2'
        >>> a
        'a > 1'
        >>> b
        'b < 2'
        """
        if not isinstance(hack, Hack):
            raise TypeError("unsupported type of %r" % hack.__class__.__name__)
        else:
            se = self.resolve()
            he = hack.resolve()
            sh = tuple(set(se) & set(he))
            if len(sh) > 0:
                raise ValueError("duplicated value %r" % sh[0])
            else:
                cln = clone(self)
                cln.group = ' AND '.join(i for i in [self.read(), hack.read()] if i != '')
                return cln

    def __eq__(self, val):
        """
        >>> h = Hack('key')
        >>> h == 3
        >>> h 
        'key = 3'
        """
        self.set('=', val)

    def __ne__(self, val):
        """
        >>> h = Hack('key')
        >>> h != 3
        >>> h
        'key != 3'
        """
        self.set('!=', val)

    def __gt__(self, val):
        """
        >>> h = Hack('key')
        >>> h > 3
        >>> h
        'key > 3'
        """
        self.set('>', val)

    def __ge__(self, val):
        """
        >>> h = Hack('key')
        >>> h >= 3
        >>> h
        'key >= 3'
        """
        self.set('>=', val)

    def __lt__(self, val):
        """
        >>> h = Hack('key')
        >>> h < 3
        >>> h
        'key < 3'
        """
        self.set('<', val)

    def __le__(self, val):
        """
        >>> h = Hack('key')
        >>> h <= 3
        >>> h
        'key <= 3'
        """
        self.set('<=', val)


class Config:
    """
    richkey: str, startswith 'tablename(alias).fieldname', endswith '__xxx' or not
    val: pyobject supports to be operated in 'SQL' syntax, usually str, int except for None, False, True
    one richkey could be 'key__lt' which endswith one tail of '__lt', 
    that means to use '<' as the operator of config object.
    (each tails mapping to the operator in 'SQL' syntax)
    richkey supports following tails (inspired by django orm):
    1. 'eq': '=', the default tail to richkey if the tail is not given
    2. 'lt': '<', less than
    3. 'lte': '<=', less than or equal 
    4. 'gt': '>' greater than
    5. 'gte': '>='
    6. 'in': 'IN', the parameter 'val' should be in a range, such as ('a', 'b', 'c')
        (especially, str object is supported e.g. 'abc')
    7. 'like': 'LIKE'
    """
    def __init__(self, richkey, val):
        self.initialize(richkey, val)

    def __repr__(self):
        return repr(self.read())

    def __and__(self, config):
        if getattr(config, '_config', None) is None:
            raise TypeError("unsupported type of %r" % config.__class__.__name__)
        else:
            _cnf = self._config
            c_cnf = config._config
            if _cnf in c_cnf:
                raise ValueError("duplicated value of %r" % c_cnf)
            else:
                cln = clone(self)
                cln._config = ' AND '.join([_cnf, c_cnf])
                return cln

    def __or__(self, config):
        if getattr(config, '_config', None) is None:
            raise TypeError("unsupported type of %r" % config.__class__.__name__)
        else:
            _cnf = self._config
            c_cnf = config._config
            if _cnf in c_cnf:
                raise ValueError("duplicated value of %r" % c_cnf)
            else:
                cln = clone(self)
                cln._config = '(' + ' OR '.join([_cnf, c_cnf]) + ')'
                return cln
        
    def initialize(self, richkey, val):
        self._resolve_richkey(richkey)
        self._handle_value(val)
        self._handle_config()

    def _handle_config(self):
        """
        To combine together key, operator and value, finally return the combination
        """
        F = '{key}{operator}{value}'
        # key
        k = self._key
        # operator
        t = self._tail or 'eq'
        opt = OPERATORS[t]
        # value
        val = v = self._value
        if t == 'in':
            val = self._handle_in(v)
        elif t == 'like':
            val = self._handle_like(v)
        else:
            val = self._handle_common(v)
        self._config = F.format(key=k, operator=opt, value=val)

    def _handle_in(self, val):
        return '(' + ', '.join(dq(e) for e in val) + ')'

    def _handle_like(self, val):
        pass

    def _handle_common(self, val):
        return val 

    def set(self, richkey, val):
        self.initialize(richkey, val)

    def read(self):
        return self._config

    @staticmethod
    def validate(val):
        return
        if (val is None) or (val is False) or (val is True):
            raise ValueError('invalid value %r' % val)

    @classmethod
    def isrich(cls, key):
        rsk = cls.resolve(key)
        if len(rsk) == 1:
            return False
        else:
            return True

    def _resolve_richkey(self, richkey):
        rsk = self.resolve(richkey)
        if len(rsk) == 1:
            self._key, = rsk
            self._tail = None
        else:
            self._key, self._tail = rsk 

    def _handle_value(self, val):
        self.validate(val)
        self._value = val 

    @staticmethod
    def resolve(richkey):
        """
        richkey: str
        resovle from --> to
        'key__tail'  --> ('key', 'tail')
        'key'        --> ('key', '')
        """
        if len(richkey) != 0:
            if '__' in richkey:
                key, tail = richkey.split('__')
                if len(key) > 0 and len(tail) > 0:
                    if tail in OPERATORS:
                        return key, tail
                    else:
                        raise ValueError('invalid richkey tail %r' % tail)
                else:
                    raise ValueError('invalid richkey %r' % richkey)
            else:
                return richkey,
        else:
            raise ValueError('invalid richkey %r' % richkey)

config = Config
# ====================
class SQL:

    def __init__(self, db):
        super(SQL, self).__init__()
        self.db = db
        self.reset()

    def __del__(self):
        if getattr(self, '_connection', None) is not None:
            self._connection.close()
            self._connection = None

    @property
    def clone(self):
        return clone(self)

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
            return clone(tb)

    def table(self, tblname, alias=''):
        if alias is '':
            alias = tblname
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

    def validate(self):
        from .database import Table
        try:
            assert isinstance(self._table, Table)
        except AssertionError:
            raise ValueError('%r is not a valid instance of Table' % self._table)

INNER_JOIN  = lambda tbl: ' INNER JOIN '.join(tbl)
DATE_FORMAT = lambda fmt: lambda f: 'DATE_FORMAT(%s, ' % f + '"%s")' % fmt

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
            each name of field is expected to be in format 'table_alias.field'
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
            tbl.append(
                '{name} AS {alias} ON {rel}'.format(
                    name=j.tb.name,
                    alias=j.tb.alias,
                    rel=j.rel)
                )
        tb = '{name} AS {alias}'.format(
            name=self._table.name,
            alias=self._table.alias)
        tbl.insert(0, tb)
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
        return cursor.queryset()

    def inner_join(self, tblname, on, alias):
        tb = self._access_table(tblname)
        tb.set_alias(alias)
        self._joints.append(Joint(tb,on))
        return self.clone

    def where(self, config):
        cln = clone(self)
        cln._where = config.read()
        return cln

    def orderby(self, field, desc=False, key=None):
        """
        field: str, name of field
        defaults to order by field, if key is given then order by value key(field) returns
        """
        into_desc = lambda f:'%s DESC' % f 
        ob = key and key(field) or field 
        if desc is True : ob = into_desc(ob)
        self._orderbys.append(ob)
        return self.clone

    def distinct(self):
        self._distinct = True
        return self.clone

    def limit(self, startpos=0, count=0):
        if startpos > 0 and count == 0:
            self._limit = 0, startpos
        elif startpos >= 0 and count > 0:
            self._limit = startpos, count
        return self.clone


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

    def where(self, config):
        cln = clone(self)
        cln._where = config.read()
        return cln

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
                if isinstance(v, StringType): f = '%s="%s"'
                l.append(f % (k, v))
            return 'SET %s' % (', '.join(l))

    def _handle_values(self):
        pass
        
    def _handle_where(self):
        return ('WHERE %s' % self._where) if self._where else None  

if __name__ == '__main__':
    import doctest
    doctest.testmod()
    # a = config('a', 1)
    # b = config('b__gt', 2)
    # c = config('c__lte', 3)
    # d = a & b & c
    # print(a.read())
    # print(b.read())
    # print(c.read())
    # print(d.read())
    # print(config.isrich('ab__lt'))
    # print(config.resolve('a__lt'))
