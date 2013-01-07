"""
datastore.py - data storage utilities.

A DataStore is a service that can store key/value pairs, where the key is
a unicode string, and the value is a Python object.

This module defines three concrete DataStore classes:
  1. AwsS3: the data is store on Amazon's Simple Storage Service.
  2. DB   : the data is stored in a SQLite database.
  3. Dir  : the data is stored as files in a directory.

All DataStore classes respect the following interface. The term "obj" (as in
getobj and setobj) refers to a Python object, that will be converted to a
binary buffer using the pickling protocol version 2. The term "data" (as in
getdata and setdata) refers to a binary buffer, that should be written into the
concrete DataStore. External clients will use getobj/setobj, while
getdata/setdata will mostly be used internally.  getobj/setobj accept
parameters to specify the encoding (e.g., compression, encryption), while
getdata/setdata do not alter the binary buffer in any way.
  1. getid()            -> returns the ID of the store (a string)
  2. getdata(key)       -> returns tuple (data, update_time)
  3. getobj(key)        -> returns tuple (obj, update_time)
  4. getupdtime(key)    -> returns the update time of the specified key
  5. setdata(key, data) -> set data, return update_time
  6. setobj(key, obj)   -> set obj, return update_time
  7. listkeys()         -> returns a dict of key/update_time pairs

The key has the following constraints:
  1. Can only contain characters that are valid Windows filename characters,
     with one exception (see next point).
  2. Can contain the forward slash character '/', that will be converted to
     sub-directories in the DirStore concrete DataStore.

todo: check all imports if they are still necessary

Copyright (c) 2012, Francois Jeannotte.
"""

import binascii
import bz2
import contextlib
import cPickle
import datetime
import functools
import hashlib
import itertools
import os
import os.path
import sqlite3
import time
import timeit
import util
import uuid
import zlib

import Crypto.Cipher.AES
import dpapi

CompAlgo    = util.Enum('uncompressed zlib bz2')
CryptAlgo   = util.Enum('unencrypted AES')
ProtectAlgo = util.Enum('unprotected DPAPI')

trace_sql = False

class DataStore(object):
  """
  Here is the layout of the header
  
  - protocol version 0:

    - byte 0: the protocol version
    - byte 1: encoding parameters:

        bits: 0 0 0         | 0 0 0          |  0 0
              comp (3 bits) | crypt (3 bits) |  protect (2 bits)
  """

  def __init__(self, comp_algo=CompAlgo.uncompressed, comp_level=5, crypt_algo=CryptAlgo.unencrypted,
               crypt_pwd=None, protect_algo=ProtectAlgo.unprotected):
    """
    Creates a datastore with default values for the three encoding parameters.
    Those parameters can be changed for each call to setobj.
    """
    assert CompAlgo.validate(comp_algo)
    assert comp_level >= 1 and comp_level <= 9
    assert CryptAlgo.validate(crypt_algo)
    assert ProtectAlgo.validate(protect_algo)

    # Create a private dict of attributes. This is used to store all internal
    # attributes. This is necessary because we override getattr and setattr.
    self.__dict__['_priv'] = dict()
    self._priv['_comp_algo']    = comp_algo
    self._priv['_comp_level']   = comp_level
    self._priv['_crypt_algo']   = crypt_algo
    self._priv['_crypt_pwd']    = crypt_pwd
    self._priv['_protect_algo'] = protect_algo

  def __getattr__(self, name):
    if name in self._priv:
      return self._priv[name]
    (obj, upd_time) = self.getobj(name)
    if obj != None:
      return obj
    raise AttributeError('key {} does not exist in the store'.format(name))  

  def __setattr__(self, name, value):
    self.setobj(name, value)

  def getobj(self, key, crypt_pwd=None):
    """
    Arguments:
      key        - The key to retrieve
      crypt_pwd  - The decryption password. Will be used only if the buffer was
                   encrypted.
    """

    # Get the binary buffer from the underlying store
    buf, upd_time = self.getdata(key)
    if not buf:
      return (None, None)

    # Get the protocol version
    proto_ver = ord(buf[0])
    assert proto_ver == 0

    # Decode the header
    header = ord(buf[1])
    buf = buf[2:]
    comp_algo    = (header & 0b11100000) >> 5
    crypt_algo   = (header & 0b00011100) >> 2
    protect_algo = (header & 0b00000011)
    assert CompAlgo.validate(comp_algo)
    assert CryptAlgo.validate(crypt_algo)
    assert ProtectAlgo.validate(protect_algo)

    # Unprotect
    if protect_algo == ProtectAlgo.DPAPI:
      buf = dpapi.decryptData(buf)

    # Decrypt
    if crypt_algo == CryptAlgo.AES:
      if crypt_pwd == None:
        crypt_pwd = self._crypt_pwd
      assert crypt_pwd
      IV = buf[:16] 
      buf = buf[16:] 
      crypt_key = hashlib.sha256(crypt_pwd).digest() 
      crypt_key = Crypto.Cipher.AES.new(crypt_key, Crypto.Cipher.AES.MODE_CFB, IV) 
      buf = crypt_key.decrypt(buf)

    # Decompress
    if comp_algo == CompAlgo.zlib:
      buf = zlib.decompress(buf)
    elif comp_algo == CompAlgo.bz2:
      buf = bz2.decompress(buf)
      
    # Unpickle the object and return it
    return cPickle.loads(buf), upd_time

  def setobj(self, key, obj, comp_algo=None, comp_level=None, crypt_algo=None, crypt_pwd=None, protect_algo=None):

    # Validate and setup parameters, retrieving defaults if necessary
    if comp_algo == None:
      comp_algo = self._comp_algo
    assert CompAlgo.validate(comp_algo)
    if comp_algo != CompAlgo.uncompressed:
      if comp_level == None:
        comp_level = self._comp_level
      assert comp_level >= 1 and comp_level <= 9
    if crypt_algo == None:
      crypt_algo = self._crypt_algo
    assert CryptAlgo.validate(crypt_algo)
    if crypt_algo != CryptAlgo.unencrypted:
      if crypt_pwd == None:
        crypt_pwd = self._crypt_pwd
      assert crypt_pwd
    if protect_algo == None:
      protect_algo = self._protect_algo
    assert ProtectAlgo.validate(protect_algo)

    # Generate the binary buffer for the object
    buf = cPickle.dumps(obj, protocol=2)

    # Apply compression, if necessary
    if comp_algo == CompAlgo.zlib:
      buf = zlib.compress(buf, comp_level)
    elif comp_algo == CompAlgo.bz2:
      buf = bz2.compress(buf, comp_level)
      
    # Apply encryption, if necessary
    if crypt_algo == CryptAlgo.AES:
      IV = os.urandom(16)
      crypt_key = hashlib.sha256(crypt_pwd).digest()
      crypt_key = Crypto.Cipher.AES.new(crypt_key, Crypto.Cipher.AES.MODE_CFB, IV)
      buf = IV + crypt_key.encrypt(buf)

    # Apply protection, if necessary
    if protect_algo == ProtectAlgo.DPAPI:
      buf = dpapi.cryptData(buf)

    # Prepend the header and write the buffer in the underlying store
    proto_ver = 0
    header_byte = (comp_algo << 5) | (crypt_algo << 2) | protect_algo
    buf = chr(proto_ver) + chr(header_byte) + buf
    return self.setdata(key, buf)

class SqliteTable(object):

  @classmethod
  def connect(cls, db_path, *tables_to_create):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    for table_class in tables_to_create:
      assert issubclass(table_class, cls)
      table_class.colnames = [c[0] for c in table_class.columns]
      query_str = 'CREATE TABLE IF NOT EXISTS [tbl_{}] ('.format(table_class.__name__)
      query_str += ','.join('[{}] {}'.format(t[0], t[1]) for t in table_class.columns)
      if hasattr(table_class, 'unique'):
        query_str += ','
        query_str += ','.join('UNIQUE ({})'.format(u) for u in table_class.unique)
      query_str += ');'
      if trace_sql:
        print query_str
      with conn:
        conn.execute(query_str)
      if hasattr(table_class, 'default_rows'):
        for row in table_class.default_rows:
          if not table_class.exists(**row):
            table_class.insert(conn, **row)
    return conn

  @classmethod
  def select(cls, conn, cols, **kwargs):
    assert set(kwargs.keys()).issubset(cls.colnames)
    if not cols:
      cols = '*'
    query_str = 'SELECT {} FROM tbl_{}'.format(cols, cls.__name__)
    if kwargs:
      query_str += ' WHERE '
      query_str += ' AND '.join('{}=?'.format(kw) for kw in kwargs)
    query_str += ';'
    if trace_sql:
      print query_str, kwargs.values()
    return conn.execute(query_str, kwargs.values())

  @classmethod
  def exists(cls, conn, **kwargs):
    return bool(cls.select(conn, **kwargs))

  @classmethod
  def insert(cls, conn, **kwargs):
    if kwargs:
      assert set(kwargs.keys()).issubset(cls.colnames), '{}, {}'.format(kwargs, cls.colnames)
      joined_colnames = ','.join("'{}'".format(c) for c in kwargs)
      query = 'INSERT INTO tbl_{} ({}) VALUES ({})'.format(cls.__name__, joined_colnames, ','.join('?' for k in kwargs))
    else:
      query = 'INSERT INTO tbl_{} DEFAULT VALUES'.format(cls.__name__)
    if trace_sql:
      print query, kwargs.values()
    with conn, contextlib.closing(conn.cursor()) as cur:
      cur.execute(query, kwargs.values())
      return cur.lastrowid

  @classmethod
  def insertmany(cls, conn, cols, tuples):
    assert set(cols).issubset(cls.colnames), '{}, {}'.format(cols, cls.colnames)
    joined_colnames = ','.join("'{}'".format(c) for c in cols)
    query = 'INSERT INTO tbl_{} ({}) VALUES ({})'.format(cls.__name__, joined_colnames, ','.join('?' for k in cols))
    if trace_sql:
      print query, cols, tuples
    with conn:
      conn.executemany(query, tuples)

  @classmethod
  def update(cls, conn, key_cols, val_cols, insert_missing=False):
    """
    Update a row in the DB, optionnally inserting if the item does not
    exist.
      - key_cols:       a dict of the columns that will be used to retrieve
                        the existing row.
      - val_cols:       values to update in the DB, but that won't be used
                        to select.
      - insert_missing: inserts a new item if it was not found.
    """
    assert set(key_cols).issubset(cls.colnames)
    assert set(val_cols).issubset(cls.colnames)
    if insert_missing and not cls.exists(conn, **key_cols):
      cls.insert(conn, **dict(key_cols.items() + val_cols.items()))
    else:
      key_str = ','.join('{}=?'.format(k) for k in key_cols)
      val_str = ','.join('{}=?'.format(k) for k in val_cols)
      query = 'UPDATE tbl_{} SET {} WHERE {}'.format(cls.__name__, val_str, key_str)
      if trace_sql:
        print query
        print val_cols.values() + key_cols.values()
      with conn:
        conn.execute(query, val_cols.values() + key_cols.values())

  @classmethod
  def updatemany(cls, conn, val_cols, key_cols, tuples):
    assert cls.colnames, cls.colnames
    assert set(key_cols).issubset(cls.colnames), (set(key_cols), cls.colnames)
    assert set(val_cols).issubset(cls.colnames)
    key_str = ','.join('{}=?'.format(k) for k in key_cols)
    val_str = ','.join('{}=?'.format(k) for k in val_cols)
    query = 'UPDATE tbl_{} SET {} WHERE {}'.format(cls.__name__, val_str, key_str)
    if trace_sql:
      print query, tuples
    with conn:
      conn.executemany(query, tuples)

  @classmethod
  def delete(cls, conn, **kwargs):
    assert set(kwargs.keys()).issubset(cls.colnames)
    query_str = 'DELETE FROM tbl_{}'.format(cls.__name__)
    if kwargs:
      query_str += ' WHERE '
      query_str += ' AND '.join('{}=?'.format(kw) for kw in kwargs)
    query_str += ';'
    if trace_sql:
      print query_str, kwargs.values()
    with conn:
      conn.execute(query_str, kwargs.values())

  @classmethod
  def deletemany(cls, conn, cols, tuples):
    assert set(cols).issubset(cls.colnames)
    query_str = 'DELETE FROM tbl_{}'.format(cls.__name__)
    if cols:
      query_str += ' WHERE '
      query_str += ' AND '.join('{}=?'.format(col) for col in cols)
    query_str += ';'
    if trace_sql:
      print query_str, tuples
    with conn:
      conn.executemany(query_str, tuples)

  @classmethod
  def count_rows(cls, conn):
    return conn.execute('SELECT COUNT(*) FROM tbl_{}'.format(cls.__name__)).fetchone()[0]

class DB_table(SqliteTable):
  columns = [('id'         , 'INTEGER PRIMARY KEY' ), 
             ('key'        , 'TEXT NOT NULL UNIQUE'), 
             ('value'      , 'BLOB NOT NULL'       ), 
             ('update_time', 'INTEGER NOT NULL'    )]

class DB(DataStore):
  """
  Store key/value pairs in a DB.
  """
  def __init__(self, db_path, **kwargs):
    super(DB, self).__init__(**kwargs)
    self._priv['_db_path'] = db_path
    self._priv['_conn']    = SqliteTable.connect(db_path, DB_table)
    if not DB_table.exists(self._conn, key='id'):
      DB_table.insert(self._conn, key='id', value=sqlite3.Binary(uuid.uuid4().bytes), update_time=time.time())

  def getid(self):
    return binascii.hexlify(DB_table.select(self._conn, key='id').value)

  def getname(self):
    return self._db_path

  def getdata(self, key):
    if key == 'id':
      raise Exception('"id" is a reserved key')
    rows = DB_table.select(self._conn, key=key)
    if rows:
      row = rows[0]
      return row.value, datetime.datetime.fromtimestamp(row.update_time)
    return None, None

  def getupdtime(self, key):
    if key == 'id':
      raise Exception('"id" is a reserved key')
    return datetime.datetime.fromtimestamp(DB_table.select(self._conn, key=key).update_time)

  def setdata(self, key, data, overwrite=True):

    # Validate parameters
    if key == 'id':
      raise Exception('"id" is a reserved key')
    if not key:
      raise Exception('key invalid: empty or None')
    if key[0] == '/':
      key = key[1:] # Remove unnecessary / at beginning of key

    DB_table.update(self._conn, {'key':key}, {'value':sqlite3.Binary(data), 'update_time':time.time()}, insert_missing=True)

  def listkeys(self, prefix=None):
    return dict((row.key, row.update_time) for row in DB_table.select(self._conn))

class Dir(DataStore):
  """
  Store key/value pairs in a directory.
  """

  def __init__(self, path):
    super(Dir, self).__init__()
    self._priv['_path'] = unicode(os.path.abspath(path))
    if not os.path.isdir(self._path):
      os.mkdir(self._path)
    id_path = os.path.join(self._path, 'id')
    if not os.path.isfile(id_path):
      with open(id_path, 'wb') as f:
        f.write(uuid.uuid4().hex)

  def getid(self):
    id_path = os.path.join(self._path, 'id')
    with open(id_path, 'rb') as f:
      return unicode(f.read())

  def getname(self):
    return self._path

  def getdata(self, key):
    if key == 'id':
      raise Exception('"id" is a reserved key')
    keys = self.listkeys()
    if key in keys:
      fname = os.path.join(self._path, key)
      with open(fname, 'rb') as f:
        return f.read(), self.getupdtime(key)

  def getupdtime(self, key):
    if key == 'id':
      raise Exception('"id" is a reserved key')
    fname = os.path.join(self._path, key)
    return datetime.datetime.fromtimestamp(os.path.getmtime(fname))

  def setdata(self, key, data):

    # Validate parameters
    if key == 'id':
      raise Exception('"id" is a reserved key')
    if not key:
      raise Exception('key invalid: empty or None')
    if key[0] == '/':
      key = key[1:] # Remove unnecessary / at beginning of key

    # Tokenize the key on / to figure out the directories to create
    data_path = self._path
    tokens = key.split('/')
    assert(len(tokens) > 0)
    file_part = tokens.pop() # The last item should represent the "file" part, e.g.,
                             # items/my_key. In that example, the last token is my_key,
                             # and it is the file. Other tokens are going to be directories.
    for token in tokens:
      data_path = os.path.join(data_path, token)
      if not os.path.isdir(data_path):
        os.mkdir(data_path)
    data_path = os.path.join(data_path, file_part)
    with open(data_path, 'wb') as f:
      f.write(data)
    return datetime.datetime.fromtimestamp(os.path.getmtime(data_path))

  def listkeys(self, prefix=None):
    keys = dict()
    for (dirpath, dirnames, filenames) in os.walk(self._path):
      for filename in filenames:
        fname = os.path.join(dirpath, filename)  
        assert(os.path.isfile(fname))
        key = os.path.relpath(fname, self._path).replace(os.sep, '/')
        if prefix and not key.startswith(prefix):
          continue
        keys[key] = datetime.datetime.fromtimestamp(os.path.getmtime(fname))
    keys.pop('id', None)
    return keys
      
def test_pack():
  print '--------------------------------'
  print 'test_dpapi'
  print '--------------------------------'
  plain = 'abcdef'
  print 'plain: ', plain
  cipher = pack(plain, b64=True, protect_algo='DPAPI')
  print 'cipher: ', cipher
  plain = unpack(cipher, b64=True)
  print 'plain: ', plain

  plain = [3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  print 'plain: ', plain
  cipher = pack(plain, b64=True, protect_algo='DPAPI')
  print 'cipher: ', cipher
  plain = unpack(cipher, b64=True)
  print 'plain: ', plain

  print '--------------------------------'
  print 'test_encrypt'
  print '--------------------------------'
  print 'Test str to binary data'
  plain = 'this is the plain text'
  print 'plain: "%s"' % (plain) 
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff')
  plain = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff')
  print 'decrypted: "%s"' % (plain)
  print
  print 'Test str to b64 data'
  plain = 'this is the plain text'
  print 'plain: "%s"' % (plain) 
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=True)
  print 'cipher: "%s"' % cipher
  plain = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=True)
  print 'decrypted: "%s"' % (plain)
  
  print
  print 'Test obj to b64 data (1)'
  plain = [3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  print 'plain: "%s"' % (plain) 
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=True)
  print 'cipher: "%s"' % cipher
  plain = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=True)
  print 'decrypted: "%s"' % (plain)

  print
  print 'Test obj to b64 data (2)'
  plain = [3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  print 'plain: "%s"' % (plain) 
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=True)
  print 'cipher: "%s"' % cipher
  plain = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=True)
  print 'decrypted: "%s"' % (plain)

  print
  print 'Test obj to b64 data (with compression)'
  plain = [3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  print 'plain: "%s"' % (plain) 
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=True, comp_algo='zlib')
  print 'cipher: "%s"' % cipher
  plain = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=True)
  print 'decrypted: "%s"' % (plain)

  print
  print 'Test large'
  plain = 9999*[3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=False)
  print 'len(cipher): %s' % len(cipher)
  plain2 = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=False)
  if plain == plain2:
    print 'decryption OK'
  else:
    print 'decryption FAIL'
  
  print
  print 'Test large with comp level 3'
  plain = 9999*[3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=False, comp_algo='zlib', comp_level=3)
  print 'len(cipher): %s' % len(cipher)
  plain2 = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=False)
  if plain == plain2:
    print 'decryption OK'
  else:
    print 'decryption FAIL'

  print
  print 'Test large with comp level 9'
  plain = 9999*[3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=False, comp_algo='zlib', comp_level=9)
  print 'len(cipher): %s' % len(cipher)
  plain2 = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=False)
  if plain == plain2:
    print 'decryption OK'
  else:
    print 'decryption FAIL'

  print
  print 'Test large with comp level 9 (with protection and checksum)'
  plain = 9999*[3, 4, 'abc', {'a': 3, 'b': 4.0, 'c': u'delta'}]
  cipher = pack(plain, crypt_algo='AES', crypt_pwd='aaabbbccc dddeeefff', b64=False, comp_algo='zlib', comp_level=9, protect_algo='DPAPI', checksum=True)
  print 'len(cipher): %s' % len(cipher)
  plain2 = unpack(cipher, crypt_pwd='aaabbbccc dddeeefff', b64=False)
  if plain == plain2:
    print 'decryption OK'
  else:
    print 'decryption FAIL'

test_crypt_objs = 10*['skdfhskdhfksdjhfgskdjvbsdhbfv', 'abc', 123123123123, 9.234234234, [1,2,3]]
test_crypt_pwd  = 'this is the passphrase to use with AES encryption performance test abc def.'

def test_dpapi_perf(b64, comp):
  if comp:
    comp = 'zlib'
  else:
    comp = None
  cipher = pack(test_crypt_objs, protect_algo='DPAPI', crypt_pwd=test_crypt_pwd, b64=b64, comp_algo=comp)
  decrypted = unpack(cipher, crypt_pwd=test_crypt_pwd, b64=b64)

def test_aes_perf(b64, comp):
  if comp:
    comp = 'zlib'
  else:
    comp = None
  cipher = pack(test_crypt_objs, crypt_algo='AES', crypt_pwd=test_crypt_pwd, b64=b64, comp_algo=comp)
  decrypted = unpack(cipher, crypt_pwd=test_crypt_pwd, b64=b64)

def test_crypt_perf():
  count = 1000
  funcs = [test_dpapi_perf, test_aes_perf]
  for func in funcs:
    print
    print func.__name__
    print
    for pair in itertools.product([False, True], repeat=2):
      print 'b64:', pair[0], 'comp:', pair[1]
      t = timeit.Timer(functools.partial(func, *pair))
      print t.timeit(count)

def test_dir_store():
  path = r'C:\home\program\script\test\datastore\test_dir_store'
  sto = Dir(path)
  sto.setobj('key1', 'abc')
  sto.setobj('key2', 823)
  print sto.getobj('key2')
  print sto.getobj('key1')
  sto.setobj('key3', {'a':3, 'b':4}, comp_algo=CompAlgo.bz2)
  print sto.getobj('key3')

  large_obj = 'sadasdasd'*99999
  sto.setobj('key_large', large_obj)
  print len(sto.getdata('key_large')[0]), len(sto.getobj('key_large')[0])
  sto.setobj('key_large_comp', large_obj, comp_algo=CompAlgo.bz2)
  print len(sto.getdata('key_large_comp')[0]), len(sto.getobj('key_large_comp')[0])
  sto.setobj('key_large_crypt', large_obj, crypt_algo=CryptAlgo.AES, crypt_pwd='sdjfhashdf')
  print len(sto.getdata('key_large_crypt')[0]), len(sto.getobj('key_large_crypt', crypt_pwd='sdjfhashdf')[0])
  sto.setobj('key_large_all', large_obj, comp_algo=CompAlgo.bz2, crypt_algo=CryptAlgo.AES, crypt_pwd='sdjfhashdfx', protect_algo=ProtectAlgo.DPAPI)
  print len(sto.getdata('key_large_all')[0]), len(sto.getobj('key_large_all', crypt_pwd='sdjfhashdfx')[0])

def test_db_store():
  path = r'C:\home\program\script\test\datastore\test_db_store.db'
  sto = DB(path)
  sto.setobj('key3', {'a':3, 'b':4}, comp_algo=CompAlgo.bz2)
  print sto.getobj('key3')
  sto.setobj('key3', 9999999, crypt_algo=CryptAlgo.AES, crypt_pwd='failure')
  obj, upd_time = sto.getobj('key3', crypt_pwd='failure')
  assert 9999999 == obj, obj
  sto2 = DB(':memory:', comp_algo=CompAlgo.bz2, crypt_algo=CryptAlgo.AES, crypt_pwd='alpha', protect_algo=ProtectAlgo.DPAPI)
  for k in range(10):
    sto2.mypwd = 'ajsbdakjsdhkj'
    sto2.mm = 89234897238749
    sto2.poisdfsfsa = {'a':3, 'b':4}
  print sto2.mypwd
  print sto2.poisdfsfsa
  print binascii.hexlify(sto2.getdata('poisdfsfsa')[0])

def main():
  #test_crypt_perf()
  #test_dir_store()
  test_db_store()

if __name__ == '__main__':
  main()

