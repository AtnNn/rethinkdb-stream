#!/usr/bin/python

from sys import argv
from os import read
from itertools import count
import rethinkdb as r

c = r.connect()

t = argv[1]

try:
    r.table_drop(t).run(c)
except r.errors.RqlError:
    pass
r.table_create(t).run(c)

try:
    for i in count():
        data = read(0, 5 * 1024)
        if not data:
            break
        r.table(t).insert({'id': i, 'chunk': r.binary(data)}).run(c, durability='soft')
finally:
  r.table(t).insert({'id': i, 'end': True}).run(c, durability='soft')

