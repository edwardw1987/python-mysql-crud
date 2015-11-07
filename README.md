# python-mysql-crud
python-mysql-crud is intended to make convenient-api for satisfying regular DQL-demand.
## hello,world
<code>from mydql import DataBase</code><br>
<code>db = DataBase(host='localhost', db='db', user='user', passwd='passwd')</code><br>
<code>rows = db.dql().setmain('tablename').queryset.all()</code><br>
## features
##### chaining-call:
<code>rows = db.dql()\\</code><br>
<code>.setmain('tableA')\\</code><br>
<code>.inner_join('tableB', on='tableA.id=tableB.aid')\\</code><br>
<code>.inner_join('tableC', on='tableB.cid=tableC.id')\\</code><br>
<code>.queryset.slice(0,10)</code>
