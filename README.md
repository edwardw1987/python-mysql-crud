# python-mysql-crud
"python-mysql-crud" is intended to make convenient-api for satisfying regular CRUD operation.
## hello,world
<code>from mycrud import DataBase</code><br>
<code>db = DataBase(host='localhost', name='db', user='user', passwd='passwd')</code><br>
<code>rows = db.dql().table('tablename').queryset.all()</code><br>
## features
##### chaining-call:
<code>rows = db.dql()\\</code><br>
<code>.table('tableA', alias='A')\\</code><br>
<code>.inner_join('tableB', on='A.id=B.aid', alias='B')\\</code><br>
<code>.inner_join('tableC', on='B.cid=C.id', alias='C')\\</code><br>
<code>.queryset.slice(0,10)</code>
