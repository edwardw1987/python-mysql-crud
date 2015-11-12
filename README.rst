# python-mysql-crud
"python-mysql-crud" is intended to make convenient-api for satisfying regular CRUD operation.
## hello,world
.. code-block:: python
from mycrud import DataBase
db = DataBase(host='localhost', name='db', user='user', passwd='passwd')
rows = db.dql().table('tablename').queryset.all()
##### chaining-call:
<code>rows = db.dql()\\</code><br>
<code>.table('tableA', alias='A')\\</code><br>
<code>.inner_join('tableB', on='A.id=B.aid', alias='B')\\</code><br>
<code>.inner_join('tableC', on='B.cid=C.id', alias='C')\\</code><br>
<code>.queryset.slice(0,10)</code>
