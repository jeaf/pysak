"""
Services for quickly locating a directory.

Copyright (c) 2012, Francois Jeannotte.
"""

import optparse
import os
import subprocess

driveKeys = dict(dropbox = 'dropbox-pp93hnjsdf323ru82f.txt',
                 lacie   = 'lacie-hbrgiwgb2ig274g.txt',
                 seagate = 'seagate-323423sjfajfj.txt',
                 usbkey  = '11iy3rg1rihgekr1k234hf.txt')

candidateDriveLetters = 'DEFGHIJ'

class MissingDir(Exception):
  pass

class Dir(object):
  def __init__(self, path, req):
    self.path = path
    self.req = req
    if os.path.isdir(path):
      self.path = os.path.abspath(path)
    elif self.req:
      raise MissingDir, 'Could not locate path (%s), but the path was marked as "required"' % (path,)
  def __str__(self):
    return self.path
  def __getattr__(self, name):
    return Dir(self.join(name), self.req)
  def exists(self):
    return os.path.exists(self.path)
  def join(self, otherPath):
    return os.path.join(self.path, otherPath)

def home(req=True):
  if 'HOME_ROOT' in os.environ:
    return Dir(os.environ['HOME_ROOT'], req)
  return Dir('C:\\home', req)

def usbkey(req=True):
  return Dir(find_drive(driveKeys['usbkey']), req)

def lacie(req=True):
  return Dir(find_drive(driveKeys['lacie']), req).home

def dropbox(req=True):
  d = Dir(r'C:\home\dropbox', req)
  if d.exists() and not os.path.isfile(d.join(driveKeys['dropbox'])):
    raise Exception('Dropbox folder found, but identifier file absent')
  return d

def seagate(req=True):
  return Dir(find_drive(driveKeys['seagate']), req)

def find_drive(driveKey):
  for letter in candidateDriveLetters:
    drive = letter + ':\\'
    if os.path.exists(drive + driveKey):
      return drive
  return 'UNK:\\'

def lookup_path(path, req=True):
  """
  Returns a Dir object constructed from the specified string. The string
  should have the following format:
  root.dir.dir.dir
  e.g.:
  home.program.application.Firefox
  """
  assert isinstance(path, basestring), type(path)
  path_components = path.split('.')
  dir = None
  if path_components[0] == 'home':
    dir = home(req)
  elif path_components[0] == 'dropbox':
    dir = dropbox(req)
  elif path_components[0] == 'usbkey':
    dir = usbkey(req)
  elif path_components[0] == 'lacie':
    dir = lacie(req)
  elif path_components[0] == 'seagate':
    dir = seagate(req)
  else:
    if req:
      raise Exception('Invalid initial path component: %s' % (path_components[0],))
    else:
      dir = Dir(path_components[0], req)
  for comp in path_components[1:]:
    dir = getattr(dir, comp)
  return dir

def main():
  """
  The main program. Allows mounting and exploring a given container.
  """

  # Parse all arguments
  usage = 'usage: %prog folder'
  parser = optparse.OptionParser(usage)
  (options, args) = parser.parse_args()

  # Validate argument count
  if len(args) != 1:
    parser.error('incorrect number of arguments')

  # Construct the requested path
  dir = lookup_path(args[0])

  # Open the file explorer on the path
  subprocess.Popen('explorer ' + str(dir))

if __name__ == '__main__':
  main()

