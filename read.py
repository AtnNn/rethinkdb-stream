#!/usr/bin/python

# ./read.py <host> <port> <name> [n]
# Stream binary data from the table 'name' starting from chunk number 'n'

from sys import argv
from os import write
import rethinkdb as r

conn = r.connect(argv[1], int(argv[2]))

table = argv[3]
db_table = r.db('streams').table(table)

try:
    i = int(argv[4])
except:
    i = None

try:
    db_table.info().run(conn)
except:
    i = 0
    write(2, 'Waiting for table ' + table + '...')
    (r.db('rethinkdb')
      .table('table_status')
      .filter({'name': table, 'db': 'streams'})
      .changes()
      .run(conn)
      .next())
    db_table.wait().run(conn)
    write(2, ' found\n')

def read():
    global i
    end = False
    changes = db_table.changes()['new_val'].run(conn)
    try:
        ended = 'end' in db_table.max(index='id').run(conn)
        if ended:
            changes.close()
    except:
        ended = False
    if not ended:
        last_i = changes.next()['id']
    else:
        last_i = None
    if not i is None:
        for row in (db_table
                    .between(i, last_i, right_bound='closed')
                    .order_by(index= 'id')
                    .run(conn)):
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
