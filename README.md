# mydql
"mydql" is a simple extension module based on "MySQLdb" which is intended to make convenient-api for satisfying regular DQL-demand.
# hello,world
from mydql import connect
db = connect(host='localhost', db='db', user='user',passwd='passwd')
dql = db.dql()
rows = dql.setmain("tablename").queryset.all()
# feature
1.chaining-call
