#!/usr/bin/env python

"""
An example of how to use pyBDB secondary index functions.

This example uses the 'taqtrade20131218.open.data' file
included in this folder, which is all the trades reported
by TAQ for market open (09:30 am) in the sample data
downloaded from: 

ftp://ftp.nyxdata.com/Historical%20Data%20Samples/Daily%20TAQ/

"""

import os.path

import pyBDB

SIDX = {} # dict of secondary indices: k=index name, v=index fn

def get_trade_price (v):
    """Parse the trade price from the value string
    and return a string representation of that float
    (or '-1.0' if the data could not be parsed or
    converted to float successfully, etc.)"""

    try:
        # according to the TAQ Trade spec,
        # the price in dollars is the 7 digits
        # starting at position 39, and the
        # cents are the 4 digits at position 46
        dollars = float(v[39:46])
        cents   = float(v[46:50]) / 10000.0

        # return a string, since BDB requires it
        return str(dollars + cents)
    except (IndexError, ValueError, TypeError):
        # something went wrong
        return '-1.0'

def process_file (db_obj, filedata):
    """A higher-order function to take the raw trade
    data from file and load it into the database in
    one transaction.

    The key is the line number (as a string), and
    the corresponding value is the full line text.

    A real production application would check that
    the line was valid, etc., but this is enough
    for an example like this."""

    for i, line in enumerate(filedata.splitlines()):
        pyBDB.put_value(db_obj, str(i), line)

def create_db (rawfile):
    try:
        # open the TAQ file and read the raw data
        f = open(rawfile, 'r')
        data = f.read()
        f.close()

        # define the secondary indices
        # create a secondary index function on trade price, named 'px'
        SIDX['px'] = lambda dbe, dbo, dbn: pyBDB.create_secondary_index(dbe, dbo, dbn, 'px', get_trade_price, pyBDB.compare_floats)
        # (create more here, as needed/desired ...)

        # create the trades database, with a secondary index on trade price
        pyBDB.withdb('trades',
                     lambda x, y: process_file(x, data),
                     secondary_index_fns=SIDX)
    except IOError:
        pass


# use the default 'taqtrade20131218.open.data' file
# in this folder to create the trades db
create_db(os.path.join(os.path.dirname(__file__), './taqtrade20131218.open.data'))

# and prompt with some usage tips
print """Now try some queries on trade price, 'px', for example:

# Get all the trades priced in between 31.75 and 32:
trade_keys = pyBDB.withdb('trades', lambda x, y: pyBDB.get_attribute_between(x, y, 'px', '31.75', '32'), secondary_example.SIDX)

# Lookup and print the results:
>>> for key in trade_keys:
...   pyBDB.withdb('trades', lambda x, y: pyBDB.get_value(x, key), secondary_example.SIDX)
... 
'093000619QLPLT            @O X00000015000000320000 000000000000005981N '
'093000818ZSDS              F  00000030000000319900N000000000000003122C '
'093000818PSDS              F  00000010000000319900N000000000000003121C '
'093000818PSDS              F  00000010000000319900N000000000000003120C '
'093000285QFWLT            Q   00000836900000319900 000000000000005382N '
'093000285QFWLT            @O X00000836900000319900 000000000000005381N '
'093000719PSDS                Q00003936800000319800N000000000000002077C '
'093000719PSDS              O  00000010000000319800N000000000000002076C '
(etc.)
"""
