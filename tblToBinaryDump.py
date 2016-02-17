import sys

from tableDumper import TableDumper

if len(sys.argv) <> 2:
    print "Usage: python csvToDump.py tableFile.tbl"
    sys.exit()

TableDumper(sys.argv[1])