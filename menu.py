"""
menu.py - simple command line menu.

This script provides services for displaying a menu on the command-line,
and handling the user choices.

Copyright (c) 2012, Francois Jeannotte.
"""

import cio

class Menu(object):
  """
  This class represents a command-line menu.
  """

  def __init__(self, callback=None):
    """
    Parameters:
      callback: a callable that will be called every time an item is selected.
                This callback won't be called if the item defines a specific
                callback.
    """

    self.callback = callback
    self.items    = list()

  def addItem(self, item):
    """
    Adds a new item to the menu.
    """

    self.items.append(item)
    if not item.callback:
      item.callback = self.callback
    return item

  def get_first_selected(self):
    """Returns the first selected items, None if none is selected."""
    for item in self.items:
      if item.selected:
        return item
    return None

  def show(self, sort=True):
    """
    Print menu items and wait for user input.
    Parameters:
      sort: if True, the menu items will be sorted by key. If False, the items
            will be displayed in the same order they were added.
    Returns:
      True when a valid item was selected, False if ESC was pressed.
    """

    # The list of available keys (used to generate a key when none has been
    # provided for a given item)
    availKeys = list('abcdefghijklmnopqrstuvwxyz')

    # Loop on all items to validate the type of items, organize by key
    itemsNoKey = []
    itemsByKey = dict()
    for item in self.items:
      if not isinstance(item, Item):
        raise Exception('Items composing the menu must be instances of the menu.Item class')
      if len(item.key) > 1:
        raise Exception('Item key cannot have more than one character')
      if item.key == '':
        itemsNoKey.append(item)
      else:
        if item.key in itemsByKey:
          raise Exception('Two items have the same key %s' % (item.key,))
        itemsByKey[item.key] = item
        availKeys.remove(item.key)

    # If some items have no key, generate a key for those
    for item in itemsNoKey:
      item.key = availKeys.pop(0)
      if item.key in itemsByKey:
        raise Exception('An auto generated key was already allocated')
      itemsByKey[item.key] = item

    # Print menu
    sortedItems = sorted(self.items) if sort else self.items
    for i, item in enumerate(sortedItems):
      item.lineNumber = cio.getcurpos().y
      print item.getLine()

    # Wait for user input and return result(s)
    result = True
    while True:
      c = cio.wait_key()
      if c in itemsByKey:
        if itemsByKey[c].trigger():
          break
      elif ord(c) == 13: # ENTER key
        break
      elif ord(c) == 27: # ESC key
        result = False
        break

    # Return result
    return result

class Item(object):
  def __init__(self, text, key='', flag=' ', actions=' *', toggle=False, obj=None, callback=None):
    """
    Parameters:
      text    : the text to display on the menu
      key     : the key (a char) to trigger the menu item. If none is provided
                (empty string), a key will automatically be provided
      flag    : A one-char flag that will be displayed on the left of the
                menu item
      actions : the list of "actions" available for this item. When the item
                triggers, it will loop between the actions.
      toggle  : If True, the menu item is allowed to toggle between all its
                actions when triggered. If False, a trigger will "commit"
                the menu (no return required)
      obj     : optional user-defined object to attach to the menu item
      callback: A callable to call when this item is selected. Overrides the
                callback defined on the menu itself, if any.
    """

    self.text     = text
    self.key      = key
    self.flag     = flag
    self.actions  = actions
    self.toggle   = toggle
    self.obj      = obj
    self.callback = callback
    self.selected = False # This will be set to True if trigger is
                          # called at least once.
  def __cmp__(self, other):
    if self.key < other.key:
      return -1
    if self.key == other.key:
      return 0
    return 1
  def __str__(self):
    strVal = self.key + ') ' + self.text
    if self.selected:
      strVal = strVal + ' (selected)'
    strVal = strVal + ' (action=' + self.actions[0] + ')'
    return strVal
  def __repr__(self):
    return '<menu.Item ' + self.key + ' ' + self.text + '>'
  def trigger(self):
    self.selected = True
    if self.callback:
      self.callback(self)
    self.actions = self.actions[1:] + self.actions[0]
    cio.putchxy(4, self.lineNumber, self.actions[0])
    return False if self.toggle else True
  def getLine(self):
    line = ' ' + self.flag + ' '
    line = line + '[' + self.actions[0] + '] '
    line = line + self.key + ') ' + self.text
    return line

def cb(item):
  print 'callback ' + repr(item)

def cb_spec(item):
  print 'callback spec ' + repr(item)

def test():
  menu = Menu(callback=cb)
  menu.addItem(Item('choix 1', toggle=True))
  menu.addItem(Item('choix 2', key='m', flag='%', actions=' abc', toggle=True, callback=cb_spec))
  menu.addItem(Item('alpha choix', flag='+'))
  res = menu.show(sort=True)
  print res
  for item in menu.items:
    print item

if __name__ == '__main__':
  test()

