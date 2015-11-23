#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-07 14:51:48
# @Last Modified by:   python
# @Last Modified time: 2015-11-23 09:42:02
__metaclass__ = type
try:
    from utils import connect, Storage, StringType, clone
    from crud import DQL, DML
except ImportError:
    from .utils import connect, Storage, StringType, clone
    from .crud import DQL, DML
        

class DataBase:

    def __init__(self, host, name, user, passwd):
        self.host = host
        self.name = name
        self.user = user
        self.passwd = passwd
        self.tables = Storage()
        self._init_db()

    def cursor(self):
        return connect(
            host=self.host,
            db=self.name,
            user=self.user,
            passwd=self.passwd).cursor()

    def _init_db(self):
        for tblname, fields in self._init_mapping():
            self._init_table(tblname, fields)

    def _init_table(self, tblname, fields):
        self.tables[tblname] = Table(name=tblname, fields=fields)

    def _init_mapping(self):
        """
        The instantiating of 'DataBase' will cause connecting to the virtual databse,
        from which executing [sql] to read the basic message. Once the dql-operation has been done,
        the db-cursor closed immediately, so that it takes slight performance-price.
        """
        with self.cursor() as cursor:
            cursor.execute('SHOW TABLES')
            tables = Storage()
            for r in cursor:
                for tbname in r.values():
                    tables[tbname] = []
                    break
            for key, val in tables:
                cursor.execute('DESC %s' % key)
                for r in cursor:
                    val.append(r['Field'])
                tables[key] = tuple(val)
        return tables

    def field(self, tblname, fieldname):
        try:
            tb = self.tables[tblname]
        except KeyError:
            raise ValueError('invalid table name %r' % tblname)
        else:
            try:
                fd = tb.fields[fieldname]
            except KeyError:
                raise ValueError('invalid field name %r' % fieldname)
            else:
                return clone(fd)

    @property
    def dql(self):
        return DQL(self)

    @property
    def dml(self):
        return DML(self)


class Table:

    """
        represents a table in database
        >>> t = Table('',['a', 'b'])
        >>> sorted(t, key=lambda x:x[0])
        [('a', <Field 'unknown.a'>), ('b', <Field 'unknown.b'>)]
    """

    def __init__(self, name, fields=[], alias=''):
        self.initialize(name, fields, alias)

    def initialize(self, name, fields, alias):
        """
        name: str
        fields: interable containing field names  
        alias: str
        """
        self.name = name or 'unknown'
        self.alias = alias
        self._init_fields(fields)

    def __iter__(self):
        return iter(self.fields)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.name)

    def _init_fields(self, fields):
        self.fields = Storage()
        for fname in fields:
            self.fields[fname] = Field(parent=self, name=fname)

    def iterfieldnames(self):
        for _, f in self:
            yield (f.mutation or '%s.%s' % (self.alias or self.name, f.name))

    def set_alias(self, alias):
        try:
            assert isinstance(alias, StringType) and len(alias) > 0
        except AssertionError:
            raise ValueError('invalid Table alias %r' % alias)
        else:
            self.alias = alias

    def add_field(self, fname):
        """
        >>> t = Table('tb')
        >>> t.add_field('fd')
        >>> t.fields
        {'fd': <Field 'tb.fd'>}
        """
        if isinstance(fname, StringType) and len(fname) > 0 and not isinstance(fname, int):
            self.fields[fname] = Field(self, fname)
        else:
            raise ValueError('invalid fieldname %r' % fname)


class Field:

    def __init__(self, parent, name):
        """
        name: str
        parent: Table object
        """
        self.initialize(parent, name)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.fullname)

    def initialize(self, parent, name):
        self.parent = parent
        self.name = name
        self._mutation = None

    @property
    def fullname(self):
        F = '%s.%s'
        a = getattr(self.parent, 'alias', '')
        n = getattr(self.parent, 'name', '')
        return F % (a or n, self.name)
        
    @property
    def mutation(self):
        return self._mutation

    def date_format(self, fmt, alias=''):
        """
        >>> t = Table('table')
        >>> f = Field(t,'field')
        >>> f.date_format('%Y-%m-%d','abc')
        "DATE_FORMAT(table.field, '%Y-%m-%d') AS abc"
        """
        mut = 'DATE_FORMAT(%s, %r) AS %s' % (
            self.fullname,
            fmt,
            alias or self.name)
        self._mutation = mut
        return mut

if __name__ == '__main__':
    import doctest
    doctest.testmod()
