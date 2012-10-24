"""
util.py - various utility classes and functions.

Copyright (c) 2012, Francois Jeannotte.
"""

import collections
import cPickle
import datetime
import functools
import hashlib
import itertools
import operator
import os
import pprint
import shutil
import subprocess
import sys
import tempfile
import time
import timeit

class Enum(object):
  def __init__(self, defaults=None, **kwargs):
    if defaults:
      for i, elem_name in enumerate(s.strip() for s in defaults.split()):
        assert not hasattr(self, elem_name)
        setattr(self, elem_name, i)
    for name,val in kwargs.iteritems():
      assert not hasattr(self, name)
      setattr(self, name, val)
    assert len(self.__dict__.values()) == len(set(self.__dict__.values()))
  def validate(self, enum_value):
    return enum_value in self.__dict__.values()
  def __repr__(self):
    s = '<Enum '
    s += ', '.join('{}={}'.format(k, getattr(self, k)) for k in sorted(self.__dict__.keys()))
    s += '>'  
    return s

class ExecCounter(object):
  def __init__(self):
    self.tot = 0
    self.cnt = 0
    self.max = float('-inf')
    self.min = float('inf')
  def __call__(self, val):
    self.tot += val
    self.cnt += 1
    self.max = max(val, self.max)
    self.min = min(val, self.min)

class ExecTimer(ExecCounter):
  def __init__(self, parent, stats):
    super(ExecTimer, self).__init__()
    self.children = dict()
    self.parent = parent
    self.stats = stats
  def get_child(self, name, stats):
    child = self.children.get(name)
    if not child:
      child = ExecTimer(self, stats)
      self.children[name] = child
    return child
  def start(self):
    self.start_time = time.clock()
  def stop(self):
    self(time.clock() - self.start_time)
    self.stats.current_timer = self.parent
  def __enter__(self):
    self.start()
    return self
  def __exit__(self, exc_type, exc_value, traceback):
    self.stop()

def itertree(root, depth=0):
  if depth == 0:
    yield 'root', root, depth
    depth = 1
  for name in sorted(root.children):
    child = root.children[name]
    yield name, child, depth
    for subchild_tuple in itertree(child, depth+1):
      yield subchild_tuple

class ExecStats(object):
  """
  Services for managing execution stats.
  """

  def __init__(self):
    self.timer_root = ExecTimer(parent=None, stats=self)
    self.timer_root.start()
    self.current_timer = self.timer_root
    self.counters = collections.defaultdict(ExecCounter)
    self.custom_stats = dict()

  def close(self):
    self.timer_root.stop()

  def timer(self, name):
    self.current_timer = self.current_timer.get_child(name, self)
    return self.current_timer

  def counter(self, name):
    return self.counters[name]

  def combine(self, other):
    assert False, "Not implemented"
    for stat_name in other.counters:
      counter = self.counters.get(stat_name)
      if not stat_name:
        counter = dict()
        counter['tot'] = val
        counter['cnt'] = 1
        counter['min'] = val
        counter['max'] = val
      self.counters[stat_name] = counter
      self.counters[stat_name]['tot'] += other.counters[stat_name]['tot']
      self.counters[stat_name]['cnt'] += other.counters[stat_name]['cnt']

  def register_custom_stat(name, stat_callable):
    self.custom_stats[name] = stat_callable

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()

  def __str__(self):

    s  = '--------------------------------------------------\n'
    s += 'Timers\n'
    for name, node, depth in itertree(self.timer_root):
      s += '  ' + 2*depth*' ' + name + ' ' + str(node.tot) + '\n'
    s += 'Counters\n'
    for name, counter in self.counters.iteritems():
      s += '  ' + name + ' ' + str(counter.tot) + '\n'
    s += '--------------------------------------------------\n'
    return s

    # Compute average
    #for sec in self.counters:
    #  self.counters[sec]['avg'] = float(self.counters[sec]['tot']) / self.counters[sec]['cnt']
    #s =  '----------------------------------------------------------------------------\n'
    #s += '%-24s %-12s %-8s %-8s %-8s %-8s\n' % ('', 'Total', 'Average', 'Count', 'Min.', 'Max.')
    #s += '----------------------------------------------------------------------------\n'
    #for sec in sorted(self.counters):
    #  c = self.counters[sec]
    #  s += "%-25s %8.4f %8.4f %8.4f %8.4f %8.4f\n" % (sec, c['tot'], c['avg'], c['cnt'], c['min'], c['max'])
    #s += '\n'
    #for sec, f in self.custom_stats.iteritems():
    #  s += str(f(self.counters)) + '\n'
    #return s

def iterlines(path):
  with open(path, 'r') as f:
    for line in f:
      yield line

def unique(iterable):
  seen = set()
  for elem in iterable:
    if elem not in seen:
      yield elem
      seen.add(elem)

def isplit(chars):
  chars = iter(chars)
  eq_space = functools.partial(operator.eq, ' ')
  ne_space = functools.partial(operator.ne, ' ')
  token = ''.join(itertools.takewhile(ne_space, itertools.dropwhile(eq_space, chars)))
  while token:
    yield token
    token = ''.join(itertools.takewhile(ne_space, itertools.dropwhile(eq_space, chars)))

def walkdir(rootdir, maxdepth=None, listfiles=True, listdirs=True, file_filter=None, file_wrapper=None, dir_wrapper=None, n=0):
  """
  Iterate on files and directories.

  Arguments:
    rootdir      - the directory to parse
    maxdepth     - max recusion depth. A maxdepth of 0 will only list files and
                   dirs directly in rootdir, i.e., no recursion
    listfiles    - Flag to determine if files are listed
    listdirs     - Flag to determine if dirs are listed
    file_filter  - callable taking one string arg (the file path), and
                   returning a boolean
    file_wrapper - if different than None, will be called and returned with
                   the file path in argument
    dir_wrapper  - if different than None, will be called and returned with
                   the dir path in argument
    n            - internal argument used for recursive calls
  """
  assert os.path.isdir(rootdir)
  if dir_wrapper:
    assert callable(dir_wrapper)
  if file_wrapper:
    assert callable(file_wrapper)
  if maxdepth != None and n > maxdepth:
    return
  try:
    items = os.listdir(rootdir)
  except Exception, ex:
    yield rootdir, ex
    return
  for item in items:
    path = os.path.join(rootdir, item)
    if os.path.isdir(path):
      if listdirs:
        if dir_wrapper:
          yield dir_wrapper(path), None
        else:
          yield path, None
      for subitem in walkdir(path, maxdepth, listfiles, listdirs, file_filter, file_wrapper, dir_wrapper, n+1):
        yield subitem
    else:
      if listfiles:
        if not file_filter or file_filter(path):
          if file_wrapper:
            yield file_wrapper(path), None
          else:
            yield path, None
        
class TempDir(object):
  """
  Allow the creation of an automatically deleted temporary dir using the
  context management protocol.
  """
  def __init__(self):
    self.tempd = tempfile.mkdtemp()
  def __enter__(self):
    return self
  def __exit__(self, exc_type, exc_value, traceback):
    shutil.rmtree(self.tempd)
  def __str__(self):
    return self.tempd

class TempFile(object):
  """
  Allow the creation of an automatically deleted temporary file using the
  context management protocol.
  """
  def __init__(self, text=False, initial_data=None):
    temp_fd, self.tempf = tempfile.mkstemp(text=text)
    if initial_data:
      os.write(temp_fd, initial_data)
    os.close(temp_fd)
  def __enter__(self):
    return self
  def __exit__(self, exc_type, exc_value, traceback):
    os.remove(self.tempf)
  def __str__(self):
    return self.tempf

def compute_md5(fullpath):
  """
  Compute the MD5 hash of the specified file.
  """

  # Validate input parameters
  if not fullpath:
    raise Exception('fullpath argument cannot be None or empty')
  if not os.path.isfile(fullpath):
    raise Exception('file %s cannot be found' % (fullpath,))

  # Compute MD5
  block_size = 0x20000
  def upd(m, data):
    m.update(data)
    return m
  with open(fullpath, "rb") as fd:
    contents = iter(lambda: fd.read(block_size), "")
    m = reduce(upd, contents, hashlib.md5())
    return m.hexdigest()

def compute_sha256(fullpath):
  """
  Compute the SHA-256 hash of the specified file.
  """

  # Validate input parameters
  if not fullpath:
    raise Exception('fullpath argument cannot be None or empty')
  if not os.path.isfile(fullpath):
    raise Exception('file %s cannot be found' % (fullpath,))

  # Compute hash
  block_size = 0x20000
  def upd(m, data):
    m.update(data)
    return m
  with open(fullpath, "rb") as fd:
    contents = iter(lambda: fd.read(block_size), "")
    m = reduce(upd, contents, hashlib.sha256())
    return m.hexdigest()

def generate_md5_report(root, report_file):
  """
  Generates a text file containing a MD5 report of the specified directory
  structure. The text file will have the following structure:
                         path_of_dir
  abcdc30234234234b243bf path_of_file
  abbb2342b34b23b42b34b2 path_of_file
                         path_of_dir
  etc
  """
  if os.path.exists(report_file):
    raise Exception('Report file %s already exists' % (report_file,))
  if not os.path.isdir(root):
    raise Exception('root %s is not a directory' % (root,))
  root = os.path.abspath(root)
  lines = []
  counter = 0
  print 'Reading folder structure...'
  start_exec = datetime.datetime.now()
  for (dirpath, dirnames, filenames) in os.walk(root):
    for dirname in dirnames:
      counter = counter + 1
      fulldirname = os.path.relpath(os.path.join(dirpath, dirname), root)
      lines.append((fulldirname, '                                '))
    for filename in filenames:
      counter = counter + 1
      fullfilename = os.path.relpath(os.path.join(dirpath, filename), root)
      lines.append((fullfilename, compute_md5(os.path.abspath(os.path.join(dirpath, filename)))))
  print 'Done. (in ' + str(datetime.datetime.now() - start_exec) + ')'
  print '\nSorting entries...'
  start_exec = datetime.datetime.now()
  sorted_lines = sorted(lines)
  print 'Done. (in ' + str(datetime.datetime.now() - start_exec) + ')'
  print '\nWriting report to file...'
  start_exec = datetime.datetime.now()
  report_md5 = hashlib.md5(cPickle.dumps(sorted_lines)).hexdigest()
  with open(report_file, 'w') as f:
    f.write('Root directory: ' + root + '\n')
    f.write('Report MD5: ' + report_md5 + '\n')
    f.write('\n')
    for line in sorted_lines:
      f.write(line[1] + ' ' + line[0] + '\n')
  print 'Done. (in ' + str(datetime.datetime.now() - start_exec) + ')'

def edit_text(initial_text=None, as_list=True):
  """
  Edits a text document, possibly with an external editor, and return it.
  Arguments:
    - initial_text: the initial text to load in the editor. If None (the
                    default), an empty string is loaded.
    - as_list     : if False, the whole document is returned as
                    a single string. Otherwise, a list of lines is returned.
  """
  init_dat = None
  if initial_text:
    init_dat = initial_text.encode('mbcs')
  with TempFile(text=True, initial_data=init_dat) as tempf:
    subprocess.call('vi.bat ' + str(tempf))
    with open(unicode(str(tempf), 'mbcs')) as f:
      if as_list:
        return [unicode(line, 'mbcs') for line in f.readlines()]
      else:
        return f.read()

def main():
  test_isplit()
  return
  if len(sys.argv) == 2:
    print compute_md5(sys.argv[1])
  elif len(sys.argv) == 3:
    generate_md5_report(sys.argv[1], sys.argv[2])
  else:
    print 'Usage incorrect.'
    print 'Usage: util root_folder report_file'

test_files = [
r'C:\WINDOWS\system32\dmloader.dll'
]

def test_md5_perf():
  for file in test_files:
    compute_md5(file)

def test_sha256_perf():
  for file in test_files:
    compute_sha256(file)

def test_hash_perf():
  count = 1

  print 'MD5:'
  t = timeit.Timer(test_md5_perf)
  print t.timeit(count)

  print 'SHA256:'
  t = timeit.Timer(test_sha256_perf)
  print t.timeit(count)

def ffilter(path):
  return path.find('test-bzr') >= 0
def test_walkdir():
  for file in walkdir('C:\\', maxdepth=3, listfiles=False, file_filter=ffilter):
    print file

def test_isplit():
  text = 'z   abc def gghgfhgdfhugdfhu   '
  for token in isplit(text):
    print token
  text = ''
  for token in isplit(text):
    print token

def test_exec_stats():
  with ExecStats() as stats:
    with stats.timer('main'):
      time.sleep(0.25)
      with stats.timer('func1'):
        time.sleep(0.5)
      with stats.timer('func2'):
        time.sleep(0.1)
        with stats.timer('func2_sub'):
          time.sleep(0.25)
  print stats

if __name__ == '__main__':
  test_walkdir()
  #test_exec_stats()
  #main()

