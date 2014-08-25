#!/usr/bin/env python

"""
Utilies for using Berkeley DB (BDB) as an embedded database, 
with secondary indicies alongside basic key/value functions.

"""

from bsddb3 import db

import config

def withdb (dbname, fn, secondary_index_fns={}):
   """Access to the datastore represented by dbname,
   which is passed a higher-order function that does
   the expected action (put/get/deleter/etc.)"""

   # open the db environment
   dbe = db.DBEnv()
   dbe.open(config.DATASTORE_FOLDER, db.DB_CREATE | db.DB_INIT_MPOOL | db.DB_INIT_LOCK)

   # open the specific datastore (db file) within the db environment
   thisDB = db.DB(dbe)
   if len(secondary_index_fns) == 0:
      # permit duplicates, but only in the absence of secondary indices
      thisDB.set_flags(db.DB_DUP | db.DB_DUPSORT)
   thisDB.open(config.dbfile(dbname), None, db.DB_BTREE, db.DB_CREATE)

   # apply the secondary indices (if any)
   secondary_indices = {} # k=index name, v=index function
   for ind_name, ind_fn in secondary_index_fns.items():
      secondary_indices[ind_name] = ind_fn(dbe, thisDB, dbname)

   result = fn(thisDB, secondary_indices)

   thisDB.close()
   for ind_name, secondary_index in secondary_indices.items():
      secondary_index.close()
   dbe.close()

   return result


#
# Internal utility functions

def _convert (fn, x):
    """Apply the given conversion function to x, and return the
    converted value, or None if there was a conversion error"""

    try:
        return fn(x)
    except (TypeError, ValueError, OverflowError):
        pass

def _compare_fn (fn, x,y):
    """Define a comparison function for strings that should be
    treated as values defined by the given conversion function (fn)
    """

    # convert the original values
    xT = _convert(fn, x)
    yT = _convert(fn, y)

    # attempt the comparison if the conversion succeeded
    if xT is not None and yT is not None:
        z = xT - yT
        if z < 0:
            return -1
        elif z > 0:
            return 1
        else:
            return 0
    else:
       # use the default comparison function
        return cmp(x,y)


#
# External utility functions

def compare_ints (x,y):
    """Define a comparison function for strings that should be
    treated as integer values"""

    return _compare_fn(int, x,y)

def compare_floats (x,y):
    """Define a comparison function for strings that should be
    treated as float values"""

    return _compare_fn(float, x,y)


#
# Basic Key-Value functions

def put_value (db_obj, k, v):
   """A higher-order function to put the value (v) for the
   corresponding key (k) in the given database object."""

   # anything can be coerced to string w/o exception
   return db_obj.put(k, str(v))

def get_value (db_obj, k, conversion_fn=None):
   """A higher-order function to get the value for the
   corresponding key (k) in the given database object,
   optionally converting the result from string, based on
   the given conversion function, if any."""

   if conversion_fn is not None:
      return _convert(conversion_fn, db_obj.get(k))
   else:
      return db_obj.get(k)

def get_values (db_obj, k, conversion_fn=None):
    """Since BDB is set to allow duplicate values for the
    same key, this higher-order function returns a list of
    values which match this key, optionally converting the
    results from string, based on the given conversion
    function, if any."""

    data = []
    cur = db_obj.cursor()
    rec = cur.get(k, db.DB_SET) # rec is a tuple: (key, value)
    if rec:
        data.append(rec[1])
    while rec:
        rec = cur.get(k, db.DB_NEXT)
        if not rec:
            break
        if rec[0] != k:
            break
        data.append(rec[1])
    cur.close()

    if conversion_fn is not None:
       return filter(None, map(lambda x: _convert(conversion_fn, x), data))
    else:
       return data

def delete_key (db_obj, k):
    """Remove everything corresponding to the
    given key (k) from the database"""

    if db_obj.has_key(k):
        db_obj.delete(k)

def delete_key_value (db_obj, k, v):
    """Remove the specific key-value pair
    from the database"""

    cur = db_obj.cursor()
    if cur.get_both(k, str(v)):
       cur.delete()
    cur.close()

def search_by_key_greater_than (db_obj, k, conversion_fn=None, output_conversion_fn=None):
    """Return a list of values which are greater than
    or equal to the given key, optionally using the
    given conversion function to convert and compare
    keys as a type other than string.
    Another option, the output conversion function,
    allows the results to be converted from string.
    """

    if conversion_fn is not None:
       target = conversion_fn(k)

    data = []
    cur = db_obj.cursor()
    rec = cur.get(k, db.DB_SET_RANGE) # rec is a tuple: (key, value)
    if rec:
        data.append(rec[1])
    while rec:
        rec = cur.get(k, db.DB_NEXT)
        if not rec:
            break
        if conversion_fn is not None:
           if conversion_fn(rec[0]) >= target:
              data.append(rec[1])
        else:
           if rec[0] >= k:
              data.append(rec[1])
    cur.close()

    if output_conversion_fn is not None:
       return filter(None, map(lambda x: _convert(output_conversion_fn, x), data))
    else:
       return data

def search_by_key_less_than (db_obj, k, conversion_fn=None, output_conversion_fn=None):
    """Return a list of values which are less than
    or equal to the given key, optionally using the
    given conversion function to convert and compare
    keys as a type other than string.
    Another option, the output conversion function,
    allows the results to be converted from string.
    """

    if conversion_fn is not None:
       target = conversion_fn(k)

    data = []
    cur = db_obj.cursor()
    rec = cur.get(k, db.DB_SET_RANGE) # rec is a tuple: (key, value)
    if rec:
       if conversion_fn is not None:
          if conversion_fn(rec[0]) <= target:
             data.append(rec[1])
       else:
          if rec[0] <= k:
             data.append(rec[1])
    while rec:
        rec = cur.get(k, db.DB_PREV)
        if not rec:
            break
        if conversion_fn is not None:
           if conversion_fn(rec[0]) <= target:
              data.append(rec[1])
        else:
           if rec[0] <= k:
              data.append(rec[1])
    cur.close()

    if output_conversion_fn is not None:
       return filter(None, map(lambda x: _convert(output_conversion_fn, x), data))
    else:
       return data

def search_by_key_between (db_obj, kLo, kHi, conversion_fn=None, output_conversion_fn=None):
    """Return a list of values which are in between
    given key ranges (lo, hi), optionally using the
    given conversion function to convert and compare
    keys as a type other than string.
    Another option, the output conversion function,
    allows the results to be converted from string.
    """

    if conversion_fn is not None:
       target = conversion_fn(kLo)

    data = []
    cur = db_obj.cursor()
    rec = cur.get(kHi, db.DB_SET_RANGE) # rec is a tuple: (key, value)
    if rec:
       if conversion_fn is not None:
          if conversion_fn(rec[0]) >= targetLo:
             data.append(rec[1])
       else:
          if rec[0] >= kLo:
             data.append(rec[1])
    while rec:
        rec = cur.get(kHi, db.DB_PREV)
        if not rec:
            break
        if conversion_fn is not None:
           if conversion_fn(rec[0]) >= target:
              data.append(rec[1])
        else:
           if rec[0] >= kLo:
              data.append(rec[1])
    cur.close()

    if output_conversion_fn is not None:
       return filter(None, map(lambda x: _convert(output_conversion_fn, x), data))
    else:
       return data


#
# Secondary Index Functions

def create_secondary_index (db_env, db_obj, db_name, index_name, parse_fn, compare_fn=None):
    """Create a secondary index object for the specified attribute
    (a parseable or selectable portion of the value in the key-value
    pair), and return the secondary object"""

    sdb = db.DB(db_env)
    sdb.set_flags(db.DB_DUP | db.DB_DUPSORT) # permit duplicates

    if compare_fn is not None: # else the default comparison is string to string
        sdb.set_bt_compare(compare_fn)

    sdb.open(config.indfile(db_name, index_name), None, db.DB_BTREE, db.DB_CREATE)
    # define how to parse/select the desired attribute
    # embedded within the value of the key-value pair
    db_obj.associate(sdb, (lambda k, v: parse_fn(v)))

    return sdb


def get_attribute (db_obj, secondary_indices, index_name, attribute, conversion_fn=None):
   """A higher-order function to get a list of keys
   whose corresponding attribute (the parseable subset
   of the value portion of the key-value pair entries
   in the db) matches for the given secondary index"""

   data = []
   if secondary_indices.has_key(index_name):

      if conversion_fn is not None:
         target = conversion_fn(attribute)
      else:
         target = attribute

      dbc = secondary_indices[index_name].cursor()
      rec = dbc.pget(attribute, db.DB_SET)
      while rec: # this rec is in 3 parts: (indexed value, key, value)         
         if conversion_fn is not None:
            rec_attribute = conversion_fn(rec[0])
         else:
            rec_attribute = rec[0]

         if rec_attribute == target:
            data.append(rec[1])
         else:
            break

         rec = dbc.pget(attribute, db.DB_NEXT)
   return data


def get_attribute_greater_than (db_obj, secondary_indices, index_name, attribute, conversion_fn=None):
   """A higher-order function to get a list of keys
   whose corresponding attribute (the parseable subset
   of the value portion of the key-value pair entries
   in the db) is greater than or equal to the given
   secondary index"""

   data = []
   if secondary_indices.has_key(index_name):

      if conversion_fn is not None:
         target = conversion_fn(attribute)
      else:
         target = attribute

      dbc = secondary_indices[index_name].cursor()
      rec = dbc.pget(attribute, db.DB_SET_RANGE)
      while rec: # this rec is in 3 parts: (indexed value, key, value)         
         if conversion_fn is not None:
            rec_attribute = conversion_fn(rec[0])
         else:
            rec_attribute = rec[0]

         if rec_attribute >= target:
            data.append(rec[1])
         else:
            break

         rec = dbc.pget(attribute, db.DB_NEXT)
   return data

def get_attribute_less_than (db_obj, secondary_indices, index_name, attribute, conversion_fn=None):
   """A higher-order function to get a list of keys
   whose corresponding attribute (the parseable subset
   of the value portion of the key-value pair entries
   in the db) is less than or equal to the given
   secondary index"""

   data = []
   if secondary_indices.has_key(index_name):

      if conversion_fn is not None:
         target = conversion_fn(attribute)
      else:
         target = attribute

      dbc = secondary_indices[index_name].cursor()
      rec = dbc.pget(attribute, db.DB_SET_RANGE)
      while rec: # this rec is in 3 parts: (indexed value, key, value)         
         if conversion_fn is not None:
            rec_attribute = conversion_fn(rec[0])
         else:
            rec_attribute = rec[0]

         if rec_attribute <= target:
            data.append(rec[1])
         else:
            break

         rec = dbc.pget(attribute, db.DB_PREV)
   return data

def get_attribute_between (db_obj, secondary_indices, index_name, attributeLo, attributeHi, conversion_fn=None):
   """A higher-order function to get a list of keys
   whose corresponding attribute (the parseable subset
   of the value portion of the key-value pair entries
   in the db) is less than or equal to the given
   secondary index"""

   data = []
   if secondary_indices.has_key(index_name):

      if conversion_fn is not None:
         target = conversion_fn(attributeLo)
      else:
         target = attributeLo

      dbc = secondary_indices[index_name].cursor()
      rec = dbc.pget(attributeHi, db.DB_SET_RANGE)
      while rec: # this rec is in 3 parts: (indexed value, key, value)         
         if conversion_fn is not None:
            rec_attribute = conversion_fn(rec[0])
         else:
            rec_attribute = rec[0]

         if rec_attribute >= target:
            data.append(rec[1])
         else:
            break

         rec = dbc.pget(attributeHi, db.DB_PREV)
   return data



