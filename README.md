# About

pyBDB is a series of helper functions for using [Berkeley DB](https://en.wikipedia.org/wiki/Berkeley_DB) (BDB) in python, on top of [bsddb3](https://pypi.python.org/pypi/bsddb3), with support for secondary indices alongside basic key/value functions.

Using BDB version 5.3.28 is recommened, since it is the last open license before Oracle switched to AGPL3 starting with BDB version 6.x.

# Installation

* Download <tt>Berkeley DB 5.3.28.tar.gz</tt> from the [list of prior releases](http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index-082944.html)

* Unpack the file and change to the <tt>build_unix </tt> directory:
   ```sh
tar xvf db-5.3.28.tar.gz
cd db-5.3.28/build_unix
```

* Build and install ([full instructions here](http://docs.oracle.com/cd/E17076_04/html/installation/build_unix.html)):
   ```sh
../dist/configure
make
sudo make install
```

* Install the [bsddb3](https://pypi.python.org/pypi/bsddb3) python library
   ```sh
sudo pip install bsddb3
```

   It should detect the right version in <tt>/usr/local/BerkeleyDB.5.3/</tt> and build against it:
   ```sh
Downloading/unpacking bsddb3
  Downloading bsddb3-6.1.0.tar.gz (340Kb): 340Kb downloaded
  Running setup.py egg_info for package bsddb3
    Found Berkeley DB 5.3 installation.
      include files in /usr/local/BerkeleyDB.5.3/include
      library files in /usr/local/BerkeleyDB.5.3/lib
      library name is libdb-5.3
    Detected Berkeley DB version 5.3 from db.h  
...
```

## Usage

### Basic Key-Value operations

* Create a database called <tt>mydb</tt> and assign a value for the <tt>hue</tt> key:

   ```python
>>> import pyBDB
>>> pyBDB.withdb('mydb', lambda x, y: pyBDB.put_value(x, 'hue', 2.43))
>>> pyBDB.withdb('mydb', lambda x, y: pyBDB.get_value(x, 'hue'))
'2.43'
```

* Convert the result to the expected/desired type:

   ```python
>>> pyBDB.withdb('mydb', lambda x, y: pyBDB.get_value(x, 'hue', float))
2.43
```

* Duplicate keys are ok, too:

   ```python
>>> pyBDB.withdb('mydb', lambda x, y: pyBDB.put_value(x, 'hue', 3.14))
>>> pyBDB.withdb('mydb', lambda x, y: pyBDB.get_values(x, 'hue'))
['2.43', '3.14']
>>> pyBDB.withdb('mydb', lambda x, y: pyBDB.get_values(x, 'hue', float))
[2.43, 3.14]
```

### Secondary Indices

Using secondary indices allows for more complex queries by defining parsing functions to select specific attributes within the string value stored in the database, and performing selects exclusively on that or those attributes.

Run the [secondary_example.py](examples/secondary_example.py) file for an example of what's possible (make sure your <tt>$PYTHONPATH</tt> includes the examples folder).

These examples use the [NYSE Daily TAQ](http://www.nyxdata.com/Data-Products/Daily-TAQ) data from [ftp://ftp.nyxdata.com/Historical Data Samples/Daily TAQ/](ftp://ftp.nyxdata.com/Historical%20Data%20Samples/Daily%20TAQ/) for illustration.

```python
>>> import pyBDB
>>> from examples import secondary_example
Now try some queries on trade price, 'px', for example:

# Get all the trades priced between 31.75 and 32:
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

```