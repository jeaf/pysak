import datastore

def assert_eq(expected, actual):
  assert expected == actual, 'expected: {}, actual: {}'.format(repr(expected), repr(actual))

class TestTable(datastore.SqliteTable):
  columns = [('id'   , 'INTEGER PRIMARY KEY'),
             ('name' , 'TEXT NOT NULL'      ),
             ('value', 'INTEGER NOT NULL'  )]

conn = datastore.SqliteTable.connect(':memory:', TestTable)
TestTable.insert(conn, name='abc', value='3')
TestTable.insert(conn, name='def', value='4')
for name,value in TestTable.select(conn, 'name,value', name='def'):
  assert_eq(4, value)

print 'All tests successful.'

