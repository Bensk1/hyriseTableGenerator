import bisect
import copy_reg
from multiprocessing import Manager, Pool
import struct
import sys
import types

SIZE_OF_TABLE_HEADER = 4

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, _pickle_method)

class TableDumper():

    def __init__(self, filename):
        with open(filename, "r") as table:
            print "Creating binary dump of file %s" % (filename)

            self.lines = table.read().splitlines()
            table.close()
            self.getTableLayout(self.lines)

            pool = Pool(self.numberOfColumns)

            print "Dumping header and metadata"

            self.dumpHeader()
            self.dumpMetadata(self.rowsInTable)

            self.dumpedColumns = Manager().Value('f', 0.0)

            print "Dumping dictionaries and attribute vectors (this may take a while)"
            pool.map(self.dumpColumn, xrange(self.numberOfColumns), 1)

    def printStatus(self):
        self.dumpedColumns.value += 1
        print "Finished %.2f%%" % (self.dumpedColumns.value / self.numberOfColumns * 100)

    def buildDictionary(self, values):
        dictionary = list(set(values))
        dictionary.sort()

        return dictionary

    def dumpAttributeVector(self, columnName, values, dictionary):
        binaryFile = open("%s.attr.dat" % (columnName), "wb")

        for value in values:
            binaryFile.write(struct.pack("I", bisect.bisect(dictionary, value) - 1))

        binaryFile.close()

    def dumpColumn(self, columnIndex):
        values = []

        self.getValues(columnIndex, values)

        dictionary = self.dumpDictionary(self.columnNames[columnIndex], values)
        self.dumpAttributeVector(self.columnNames[columnIndex], values, dictionary)

        self.printStatus()

    def dumpDictionary(self, columnName, values):
        dictionary = self.buildDictionary(values)

        binaryFile = open("%s.dict.dat" % (columnName), "wb")
        binaryFile.write(struct.pack("Q", len(dictionary)))

        if type(dictionary[0]) == int:
            self.writeIntDictToFile(binaryFile, dictionary)
        elif type(dictionary[0]) == float:
            self.writeFloatDictToFile(binaryFile, dictionary)
        else:
            self.writeStringDictToFile(binaryFile, dictionary)

        binaryFile.close()
        return dictionary

    def dumpHeader(self):
        binaryFile = open("header.dat", "wb")

        binaryFile.write("%s\n" % (self.lines[0]))

        newDatatypes = []
        for datatype in self.datatypes:
            newDatatypes.append("%s_MAIN" % (datatype))

        datatypes = "|".join(newDatatypes)
        binaryFile.write("%s\n" % (datatypes))

        binaryFile.write("%s\n" % (self.lines[2]))
        binaryFile.write("%s\n" % (self.lines[3]))

        binaryFile.close()

    def dumpMetadata(self, rowsInTable):
        metadataFile = open("metadata.dat", "w")
        metadataFile.write("%i" % (rowsInTable))
        metadataFile.close()

    def getTableLayout(self, lines):
        self.columnNames = lines[0].split("|")
        self.numberOfColumns = len(self.columnNames)
        self.datatypes = lines[1].split("|")
        self.rowsInTable = len(self.lines) - SIZE_OF_TABLE_HEADER

    def getValues(self, columnIndex, values):
        if self.datatypes[columnIndex] == "INTEGER":
            for line in self.lines[SIZE_OF_TABLE_HEADER:]:
                values.append(int(line.split('|')[columnIndex]))
        elif self.datatypes[columnIndex] == "FLOAT":
            for line in self.lines[SIZE_OF_TABLE_HEADER:]:
                values.append(float(line.split('|')[columnIndex]))
        else:
            for line in self.lines[SIZE_OF_TABLE_HEADER:]:
                values.append(line.split('|')[columnIndex])

    def writeIntDictToFile(self, file, dictionary):
        for dictItem in dictionary:
            file.write(struct.pack("q", dictItem))

    def writeFloatDictToFile(self, file, dictionary):
        for dictItem in dictionary:
            file.write(struct.pack("f", dictItem))

    def writeStringDictToFile(self, file, dictionary):
        for dictItem in dictionary:
            file.write(struct.pack("Q", len(dictItem)))
            file.write(dictItem)

if len(sys.argv) <> 2:
    print "Usage: python csvToDump.py tableFile.tbl"
    sys.exit()

TableDumper(sys.argv[1])

    