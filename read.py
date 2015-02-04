#!/usr/bin/python

from sys import argv
from os import write
import rethinkdb as r

c = r.connect()

t = argv[1]
try:
    i = int(argv[2])
except:
    i = None

try:
    r.table(t).info().run(c)
except:
    i = 0
    write(2, 'Waiting for table ' + t + '...')
    (r.db('rethinkdb')
      .table('table_status')
      .filter({'name':t})
      .changes()
      .run(c)
      .next())
    r.table(t).wait().run(c)
    write(2, ' found\n')

if i is None:
    def read():
        for row in ():
            if not 'end' in row:
                yield row['chunk']
def read():
    global i
    end = False
    changes = r.table(t).changes()['new_val'].run(c)
    try:
        ended = 'end' in r.table(t).max(index='id').run(c)
        if ended:
            changes.close()
    except:
        ended = False
    if not ended:
        last_i = changes.next()['id']
    else:
        last_i = None
    if not i is None:
        for row in (r.table(t)
                    .between(i, last_i, right_bound='closed')
                    .order_by(index= 'id')
                    .run(c)):
            i = row['id']
            if 'end' in row:
                end = True
                break
            else:
                yield row['chunk']
    if not end and not ended:
        future = {}
        for row in changes:
            future[row['id']] = row
            while last_i + 1 in future:
                last_i = last_i + 1
                row = future[last_i]
                del future[last_i]
                if 'end' in row:
                    changes.close()
                else:
                    yield row['chunk']

for chunk in read():
    write(1, chunk)
