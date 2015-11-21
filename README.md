```python
from mycrud import DataBase
db = DataBase(host='localhost', name='db', user='user', passwd='passwd')
rows = db.dql.table('tablename', alias='t').queryset.all()

```

```python

rows = db.dql.table('tableA', alias='A')\
.inner_join('tableB', on='A.id=B.aid', alias='B')\
.inner_join('tableC', on='B.cid=C.id', alias='C')\
.queryset.slice(0,10)
```
###Example
```python
>>> from mycrud.utils import QuerySet
>>> details = [
    {'name':'Alice','age':18,'gender':'female'},
    {'name':'Bob','age':22,'gender':'male'},
    {'name':'Chris','age':18,'gender':'female'},
    {'name':'David','age':20,'gender':'male'},
    {'name':'Edward','age':18,'gender':'male'},
    {'name':'Fiona','age':22,'gender':'female'}]
>>> qs = QuerySet(details)
>>> for k, l in qs.groupby('age'):
        print(k)
        for e in l:
            print(e)
18
{'gender': 'female', 'age': 18, 'name': 'Alice'}
{'gender': 'female', 'age': 18, 'name': 'Chris'}
{'gender': 'male', 'age': 18, 'name': 'Edward'}
20
{'gender': 'male', 'age': 20, 'name': 'David'}
22
{'gender': 'male', 'age': 22, 'name': 'Bob'}
{'gender': 'female', 'age': 22, 'name': 'Fiona'}

