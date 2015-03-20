#!/usr/bin/python

# ./write.py <host> <port> <name>

# Stream data into the table 'name' in 1k chunks

from sys import argv
from os import read
from itertools import count
import rethinkdb as r

conn = r.connect(argv[1], int(argv[2]))

table = argv[3]

try:
    r.db_create('streams').run(conn)
except:
    pass

try:
    r.db('streams').table_drop(table).run(conn)
except r.errors.RqlError:
    pass
r.db('streams').table_create(table).run(conn)

try:
    for i in count():
        data = read(0, 1024)
        if not data:
            break
        r.db('streams').table(table).insert({'id': i, 'chunk': r.binary(data)}).run(conn, durability='soft')
finally:
  r.db('streams').table(table).insert({'id': i, 'end': True}).run(conn, durability='soft')

