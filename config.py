#!/usr/bin/env python

"""
Configuration settings for pyBDB functions

"""

import os.path

DATASTORE_FOLDER = '/tmp'
DATASTORE_FILEXT = '.bdb'

def dbfile (dbname):
    """Define the full path to the BDB file,
    given the database name, and the above
    definitions, e.g.:

    dbname = 'settings' -> /tmp/settings.bdb

    """

    return os.path.join(DATASTORE_FOLDER,
                        u''.join([dbname,
                                  DATASTORE_FILEXT]))

def indfile (dbname, indname):
    """Define the full path to the secondary
    index file, using a combination of the
    db name and index name"""

    return dbfile( u'_'.join([dbname, indname]) )


# define any local configurations in a 
# separate 'local_config.py' file which
# is not checked into source control
 
try:
    from local_config import *
except ImportError:
    pass
