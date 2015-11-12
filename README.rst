
**Hello, world**

.. code-block:: python

  from mycrud import DataBase
  db = DataBase(host='localhost', name='db', user='user', passwd='passwd')
  rows = db.dql().table('tablename').queryset.all()

**Features**

*1. chaining-call*

.. code-block:: python

    rows = db.dql()\
    .table('tableA', alias='A')\
    .inner_join('tableB', on='A.id=B.aid', alias='B')\
    .inner_join('tableC', on='B.cid=C.id', alias='C')\
    .queryset.slice(0,10)
