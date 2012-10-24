# coding=latin-1

"""
cio.py - console IO utilities for Windows.

Copyright (c) 2012, Francois Jeannotte.
"""

import ctypes
import itertools
import msvcrt
import os
import sys
import time

import win32console

class COORD(ctypes.Structure):
  _fields_ = [("x", ctypes.c_short),
              ("y", ctypes.c_short)]

class SMALL_RECT(ctypes.Structure):
  _fields_ = [("left"  , ctypes.c_short),
              ("top"   , ctypes.c_short),
              ("right" , ctypes.c_short),
              ("bottom", ctypes.c_short)]
              
class CONSOLE_SCREEN_BUFFER_INFO(ctypes.Structure):
  _fields_ = [("dwSize"             , COORD),
              ("dwCursorPosition"   , COORD), 
              ("wAttributes"        , ctypes.c_ushort), 
              ("srWindow"           , SMALL_RECT), 
              ("dwMaximumWindowSize", COORD)]

k32 = ctypes.windll.kernel32
stdout = k32.GetStdHandle(-11)

def getcurpos():
  """
  Returns the cursor position as a COORD.
  """
  conInfo = CONSOLE_SCREEN_BUFFER_INFO()
  k32.GetConsoleScreenBufferInfo(stdout, ctypes.byref(conInfo)) 
  return conInfo.dwCursorPosition

def setcurpos(x, y):
  """
  Sets the cursor position.
  """
  k32.SetConsoleCursorPosition(stdout, COORD(x, y))
  
def putchxy(x, y, ch):
  """
  Puts a char at the specified position. The cursor position is not affected.
  """
  prevCurPos = getcurpos()
  setcurpos(x, y)
  msvcrt.putch(ch)
  setcurpos(prevCurPos.x, prevCurPos.y)
  
def wait_key():
  """
  Waits until a key is pressed. Returns the key.
  """
  while True:
    if msvcrt.kbhit():
      return msvcrt.getch()
    else:
      time.sleep(0.01)

def set_text_color(colors=None):
  """
  Sets the text color on the console. Calling this function with None (the
  default) will restore default colors. The colors must be a iterable of
  strings.
  """

  flags = 0

  # If colors is None, use defaults colors
  if not colors:
    flags = win32console.FOREGROUND_BLUE | win32console.FOREGROUND_GREEN | win32console.FOREGROUND_RED

  # colors is set, process it
  else:

    # If colors is a single string, use this as the single flag
    if isinstance(colors, basestring):
      flags = win32console.__dict__[colors]

    # Otherwise, consider colors a list of strings
    else:
      for color in colors:
        flags = flags | win32console.__dict__[color]

  # Set the color
  h = win32console.GetStdHandle(win32console.STD_OUTPUT_HANDLE)
  h.SetConsoleTextAttribute(flags)

def write_color(text, colors, endline=False):
  """
  Prints the specified text, without endline, with the specified colors.
  After printing, the default color is restored.
  """
  text = unicode(text)
  set_text_color(colors)
  sys.stdout.write(text.encode('cp850'))
  if endline:
    sys.stdout.write('\n')
  set_text_color()

def get_console_size():
  """ Returns a (X, Y) tuple. """
  h = win32console.GetStdHandle(win32console.STD_OUTPUT_HANDLE)
  x = h.GetConsoleScreenBufferInfo()['MaximumWindowSize'].X
  y = h.GetConsoleScreenBufferInfo()['MaximumWindowSize'].Y
  return (x,y)

def cls():
  """ Clears the screen. """
  os.system('cls')

def main():
  print "Testing module"
  print
  set_text_color('FOREGROUND_RED')
  print "Should be red"
  set_text_color()
  print "Should be default"
  write_color(u'should be greenéàöç\n', 'FOREGROUND_GREEN')
  print "Should be default"
  write_color('should be intense\n', 'FOREGROUND_INTENSITY')
  print 'Console (x,y): ', get_console_size()

  print
  print 'Colors demo:'
  colors = 'FOREGROUND_RED FOREGROUND_GREEN FOREGROUND_BLUE FOREGROUND_INTENSITY BACKGROUND_RED BACKGROUND_GREEN BACKGROUND_BLUE BACKGROUND_INTENSITY'.split()
  for i in range(1, 4):
    print i
    for combo in itertools.combinations(colors, i):
      write_color(str(combo), combo, endline=True)

if __name__ == '__main__':
  main()
  
